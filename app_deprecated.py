import os
import ssl
from gevent import monkey, spawn
import requests
import tempfile
import io
import json
import logging
from datetime import timedelta
import time
import urllib.parse

from flask import Flask, session, redirect, url_for, flash, request, jsonify
from flask_socketio import SocketIO
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix

from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from urllib3.exceptions import NewConnectionError, MaxRetryError

import uuid

import platform
from dotenv import load_dotenv

load_dotenv()

#os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

# Logger setup
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

logger.info(f"Running on dyno: {platform.node()}")

upload_results = {}

# Flask setup
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "dev")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=2, x_host=2)

# DB setup
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///state_store.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')
db = SQLAlchemy(app)

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/youtube.upload",
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtubepartner",
    "https://www.googleapis.com/auth/drive",
]

UPLOADED_VIDEOS_FILE = "uploaded_videos.json"

# OAuth state model
class OAuthStateDb(db.Model):
    state_db = db.Column(db.String(128), primary_key=True)
    session_id_db = db.Column(db.String(128))
    timestamp_db = db.Column(db.Float)
    file_url_db = db.Column(db.String(2048))
    title_db = db.Column(db.String(512))
    category_db = db.Column(db.String(64))
    status_db = db.Column(db.String(64))
    notify_db = db.Column(db.String(16))
    publishAt_db = db.Column(db.String(64))
    channelID_db = db.Column(db.String(64))
    recordid_db = db.Column(db.String(64))
    description_db = db.Column(db.Text)
    tags_db = db.Column(db.Text)  
    credentials_json = db.Column(db.Text)

try:
    with app.app_context():
        db.create_all()
except Exception as e:
    print(f"Database already initialized: {e}")


# Load/save uploaded video data
@app.before_request
def before_request():
    # Store the start time of the request to monitor duration
    request.start_time = time.time()

# Custom function to check the request duration and handle timeout
def check_timeout():
    # Allow a maximum time limit of 30 seconds (30000 ms)
    if time.time() - request.start_time > 30:
        # Log the timeout error
        logger.error(f"Connection error: Please verify your network. Request took longer than 30 seconds.")
        return jsonify({"error": "Connection error: Please verify your network."}), 408  # 408 is the HTTP code for request timeout

@app.after_request
def after_request(response):
    # Check if the request has timed out before responding
    check_timeout()
    return response

def load_uploaded_videos():
    if not os.path.exists(UPLOADED_VIDEOS_FILE):
        return {}
    with open(UPLOADED_VIDEOS_FILE, "r") as f:
        return json.load(f)

def save_uploaded_videos(uploaded):
    with open(UPLOADED_VIDEOS_FILE, "w") as f:
        json.dump(uploaded, f, indent=2)

def get_client_secrets():
    client_secrets_json = os.environ.get("CLIENT_SECRETS_JSON")
    if not client_secrets_json:
        logger.error("CLIENT_SECRETS_JSON environment variable not set.")
        raise ValueError("CLIENT_SECRETS_JSON environment variable not set.")
    try:
        config = json.loads(client_secrets_json)
        if "web" not in config:
            raise ValueError("CLIENT_SECRETS_JSON must contain a 'web' key.")
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse CLIENT_SECRETS_JSON: {e}")
        raise

def fetch_token_with_retry(flow, authorization_response, retries=3, delay=5):
    quotaguard_url = os.environ.get("QUOTAGUARDSTATIC_URL")
    session = requests.Session()
    if quotaguard_url:
        session.proxies.update({"http": quotaguard_url, "https": quotaguard_url})

    for i in range(retries):
        try:
            flow._session = session
            flow.fetch_token(authorization_response=authorization_response)
            credentials = flow.credentials

            logger.info(f"Access token: {credentials.token}")

            return credentials
        except (NewConnectionError, MaxRetryError) as e:
            logger.warning(f"Network error during token fetch (attempt {i+1}): {e}")
            if i < retries - 1:
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            logger.error(f"Error during token fetch: {e}")
            raise

@app.route("/")
def index():
    return redirect(url_for("upload_video"))

@app.route("/upload_video")
def upload_video():
    # Extract query params
    logger.info(f"Running on dyno: {platform.node()}")
    file_url = request.args.get("url")
    title = request.args.get("title", "Untitled Video")
    category = request.args.get("category", "22")
    status = request.args.get("status", "private")
    notify_subs = request.args.get("notification", "false")
    publish_at = request.args.get("publishAt")
    channelID = request.args.get("channelID")
    recordid = request.args.get("recordid")
    description = request.args.get("description", "Uploaded from Google Drive")
    tags = request.args.get("tags", "")

    if not file_url:
        return "Missing 'url' parameter", 400
    
    if channelID :
        channelID = channelID.split("/")[0]

    session_id = str(uuid.uuid4())

    logger.info(f"session_id : {session_id}")

    temp_state = str(uuid.uuid4())

    oauth_state = OAuthStateDb(
        state_db=temp_state,
        session_id_db=session_id,
        timestamp_db=time.time(),
        file_url_db=file_url,
        title_db=title,
        category_db=category,
        status_db=status,
        notify_db=notify_subs,
        publishAt_db=publish_at,
        channelID_db = channelID,
        recordid_db = recordid,
        description_db=description,
        tags_db=tags,
    )
    db.session.add(oauth_state)
    db.session.commit()

    logger.info(f"db before authorize : {OAuthStateDb.query.all()}")
    logger.info(f"Session ID sent by upload_video = {session_id}")

    return redirect(url_for("authorize", session_id_sent=session_id))

@app.route("/authorize")
def authorize():
    session_id_sent = request.args.get("session_id_sent")
    logger.info(f"session_id_sent received in /authorize: {session_id_sent}")
    
    if not session_id_sent:
        return "Missing session_id", 400

    db_row = OAuthStateDb.query.filter_by(session_id_db=session_id_sent).first()
    data = OAuthStateDb.query.all()
    logger.info(f"db_row : {db_row}")
    logger.info(f"db_row : {data}")
    if not db_row:
        return "Invalid session_id", 400

    client_secrets = get_client_secrets()
    redirect_uri = url_for("oauth2callback", _external=True)
    flow = Flow.from_client_config(client_secrets, scopes=SCOPES, redirect_uri=redirect_uri)
    
    # Generate unique state per request
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true")

    # Save state for session_id in DB
    db_row.state_db = state
    db.session.commit()

    return {"auth_url": auth_url, "state": state}

@app.route("/oauth2callback")
def oauth2callback():
    state = request.args.get("state")
    if not state:
        return "Missing state parameter", 400

    # Find matching db row by state
    db_row = OAuthStateDb.query.filter_by(state_db=state).first()
    if not db_row:
        return "Invalid state parameter", 400

    client_secrets = get_client_secrets()
    redirect_uri = url_for("oauth2callback", _external=True)
    flow = Flow.from_client_config(client_secrets, scopes=SCOPES, state=state, redirect_uri=redirect_uri)

    try:
        credentials = fetch_token_with_retry(flow, request.url)
    except Exception as e:
        return f"Token fetch failed: {e}", 500

    # Store credentials
    creds_dict = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": client_secrets["web"]["client_id"],
        "client_secret": client_secrets["web"]["client_secret"],
        "scopes": credentials.scopes
    }

    db_row.credentials_json = json.dumps(creds_dict)
    db.session.commit()

    return redirect(url_for("final_upload", session_id=db_row.session_id_db))

def upload_video_in_background(db_row, file_id, creds_dict, session_id):
    logger.info("Started video upload")
    try:
        creds = Credentials(**creds_dict)
        drive_service = build('drive', 'v3', credentials=creds)
        youtube_service = build('youtube', 'v3', credentials=creds)
        webhook_url = "https://hook.eu1.make.com/meg7mme3bo1fgmuin2e2be1e1uayn5i9"
        content_owner = os.environ.get("CONTENT_OWNER")
        result_url = None

        # Fetch video metadata from Google Drive
        file = drive_service.files().get(fileId=file_id, fields="id, name, size, mimeType, permissions").execute()
        if file.get("mimeType") != "video/mp4":
            socketio.emit("upload_progress", {"percent": 0, "message": f"{file['name']} is not an MP4 video."}, room=session_id)
            return

        file_request = drive_service.files().get_media(fileId=file["id"])
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")
        downloader = MediaIoBaseDownload(temp_file, file_request)
        done = False
        previous = -5
        while not done:
            status_obj, done = downloader.next_chunk()
            if status_obj:
                percent = int(status_obj.progress() * 100)
                socketio.emit("download_progress", {"percent": percent}, room=session_id)
                logger.info(f"download_progress : {percent} %")
                if db_row.recordid_db :
                    params = {
                        "recordid": db_row.recordid_db,
                        "status": 200,
                        "type": "download",
                        "value": percent/100
                    }
                    try:
                        if percent // 5 > previous // 5:
                            previous = percent
                            response = requests.post(webhook_url, params=params)
                            
                            if response.status_code == 200:
                                logger.info(f"Webhook successfully called at {percent}% for download progress.")
                            else:
                                logger.warning(f"Webhook responded with status: {response.status_code} at {percent}% for download progress.")
                    except Exception as e:
                        logger.warning(f"Error calling webhook for download progress. : {e}")

                else :
                    logger.warning("record_id empty for download")

            else :
                if db_row.recordid_db :
                    params = {
                        "recordid": db_row.recordid_db,
                        "status": 200,
                        "type": "download",
                        "value": 1
                    }
                    try:
                        response = requests.post(webhook_url, params=params)
                        
                        if response.status_code == 200:
                            logger.info("Webhook successfully called for upload progress.")
                        else:
                            logger.warning(f"Webhook responded with status: {response.status_code} for upload progress.")
                    except Exception as e:
                        logger.warning(f"Error calling webhook for upload progress. : {e}")

                else :
                    logger.warning("record_id empty for download")

        temp_file.close()
        # Prepare for YouTube upload
        media = MediaIoBaseUpload(open(temp_file.name, "rb"), mimetype="video/mp4", resumable=True)
        
        status_dict = {
            "privacyStatus": db_row.status_db,
            "selfDeclaredMadeForKids": False,
            "notifySubscribers": str(db_row.notify_db).lower() in ["true", "1", "yes"]
        }

        if db_row.status_db == "scheduled" and db_row.publishAt_db:
            status_dict["publishAt"] = db_row.publishAt_db

        body = {
            "snippet": {
                "title": db_row.title_db or file["name"],
                "description": db_row.description_db or "Uploaded from Google Drive",
                "tags": db_row.tags_db.split(",") if db_row.tags_db else [],
                "categoryId": db_row.category_db or "22"
            },
            "status": status_dict
        }

        if db_row.channelID_db :
            if db_row.channelID_db.startswith("UC") and len(db_row.channelID_db) == 24 :
                request_upload = youtube_service.videos().insert(
                    part="snippet,status",
                    body=body,
                    media_body=media,
                    onBehalfOfContentOwner=content_owner,
                    onBehalfOfContentOwnerChannel=db_row.channelID_db
                )
            else :
                logger.error("Invalid channelID sent in final_upload")
                return f"ChannelID error : Wrong value, please verify the channelID : {db_row.channelID_db}"
        else:
            logger.info("No channelID sent in final_upload, publishing to main channel")
            request_upload = youtube_service.videos().insert(
                part="snippet,status",
                body=body,
                media_body=media
            )

        upload_response = None
        previous = -5
        while upload_response is None:
            status_obj, upload_response = request_upload.next_chunk()
            if status_obj:
                percent = int(status_obj.progress() * 100)
                socketio.emit("upload_progress", {"percent": percent}, room=session_id)
                logger.info(f"Upload progress : {percent} %")
                
                if db_row.recordid_db :
                    params = {
                        "recordid": db_row.recordid_db,
                        "status": 200,
                        "type": "upload",
                        "value": percent/100
                    }
                    try:
                        if percent // 5 > previous // 5:
                            previous = percent
                            response = requests.post(webhook_url, params=params)
                            
                            if response.status_code == 200:
                                logger.info(f"Webhook successfully called at {percent}% for upload progress.")
                            else:
                                logger.warning(f"Webhook responded with status: {response.status_code} at {percent}% for upload progress.")
                    except Exception as e:
                        logger.warning(f"Error calling webhook for upload progress. : {e}")

                else :
                    logger.warning("record_id empty for upload")

            else :                
                if db_row.recordid_db :
                    params = {
                        "recordid": db_row.recordid_db,
                        "status": 200,
                        "type": "upload",
                        "value": 1
                    }
                    try:
                        response = requests.post(webhook_url, params=params)
                        
                        if response.status_code == 200:
                            logger.info("Webhook successfully called for upload progress.")
                        else:
                            logger.warning(f"Webhook responded with status: {response.status_code} for upload progress.")
                    except Exception as e:
                        logger.warning(f"Error calling webhook for upload progress. : {e}")

                else :
                    logger.warning("record_id empty for upload")

        result_url = f"https://youtube.com/watch?v={upload_response['id']}"
        logger.info(f"{result_url}")

        logger.info(db_row.recordid_db)

        if db_row.recordid_db and result_url:
            params = {
                "recordid": db_row.recordid_db,
                "status": 200,
                "yturl": result_url,
                "type": "upload",
                "value": 1
            }
            try:
                response = requests.post(webhook_url, params=params)
                
                if response.status_code == 200:
                    logger.info("Webhook successfully called.")
                    try:
                        os.remove(temp_file.name)
                    except Exception as cleanup_error:
                        logger.warning(f"Could not delete temporary file: {cleanup_error}")
                else:
                    logger.warning(f"Webhook responded with status: {response.status_code}")
            except Exception as e:
                logger.warning(f"Error calling webhook: {e}")

            socketio.emit("upload_complete", {"yt_url": result_url}, room=session_id)

    except HttpError as e:
        logger.error(f"API error during upload: {e._get_reason()}")
        socketio.emit("upload_error", {"error": f"API error during upload: {e._get_reason()}"}, room=session_id)
    except Exception as e:
        logger.exception("Unexpected error during upload.")
        socketio.emit("upload_error", {"error": f"Unexpected error: {e}"}, room=session_id)

    finally:
        if result_url:
            upload_results[session_id] = {
                "status": "success",
                "yt_url": result_url
            }
            return {"status": "success", "yt_url": result_url}
        else:
            upload_results[session_id] = {
                "status": "error",
                "error_message": "Upload failed"
            }
            return {"status": "error", "error_message": "Upload failed"}
        

@app.route("/final_upload")
def final_upload():
    session_id = request.args.get("session_id")
    if not session_id:
        return "Invalid session ID", 400

    db_row = OAuthStateDb.query.filter_by(session_id_db=session_id).first()
    if not db_row:
        return "Session not found in database", 404

    creds_dict = json.loads(db_row.credentials_json)

    try:
        file_id = db_row.file_url_db.split("/d/")[1].split("/")[0]
    except Exception:
        return "Invalid file URL format", 400

    logger.info("Started thread")
    spawn(upload_video_in_background, db_row, file_id, creds_dict, session_id)

    return "Video upload started", 200

@app.route("/logout")
def logout():
    session_id = request.args.get("session_id")
    if not session_id:
        logger.warning("Logout attempted without session_id")
        return "Missing session_id", 400

    return redirect(url_for("index"))

if __name__ == "__main__":
    monkey.patch_all()

    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
