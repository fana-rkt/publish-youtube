import os
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

import ssl
from gevent import monkey, spawn
import requests
import tempfile
import io
import base64
import json
import logging
from datetime import timedelta
import time
import urllib.parse

from flask import Flask, session, redirect, url_for, flash, request, jsonify
from flask_socketio import SocketIO
from flask_sqlalchemy import SQLAlchemy
from werkzeug.middleware.proxy_fix import ProxyFix

from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google.oauth2 import service_account
from googleapiclient.errors import HttpError
from urllib3.exceptions import NewConnectionError, MaxRetryError

import uuid
import platform
from dotenv import load_dotenv

load_dotenv()

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

class OAuthStateDb(db.Model):
    session_id_db = db.Column(db.String(128), primary_key=True)
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

try:
    with app.app_context():
        db.create_all()
except Exception as e:
    print(f"Database already initialized: {e}")

@app.before_request
def before_request():
    request.start_time = time.time()

def check_timeout():
    if time.time() - request.start_time > 30:
        logger.error(f"Connection error: Request took longer than 30 seconds.")
        return jsonify({"error": "Connection error: Please verify your network."}), 408

@app.after_request
def after_request(response):
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

def get_service_account_credentials():
    sa_json_b64 = os.environ.get("SERVICE_ACCOUNT_JSON_B64")
    if not sa_json_b64:
        raise ValueError("SERVICE_ACCOUNT_JSON_B64 environment variable not set")
    try:
        decoded_json = base64.b64decode(sa_json_b64).decode('utf-8')
        sa_info = json.loads(decoded_json)
        creds = service_account.Credentials.from_service_account_info(sa_info, scopes=SCOPES)
        return creds
    except Exception as e:
        logger.error(f"Failed to load service account credentials: {e}")
        raise

@app.route("/")
def index():
    return redirect(url_for("upload_video"))

@app.route("/upload_video")
def upload_video():
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

    if channelID:
        channelID = channelID.split("/")[0]

    session_id = str(uuid.uuid4())

    logger.info(f"session_id : {session_id}")

    db_row = OAuthStateDb(
        session_id_db=session_id,
        timestamp_db=time.time(),
        file_url_db=file_url,
        title_db=title,
        category_db=category,
        status_db=status,
        notify_db=notify_subs,
        publishAt_db=publish_at,
        channelID_db=channelID,
        recordid_db=recordid,
        description_db=description,
        tags_db=tags,
    )
    db.session.add(db_row)
    db.session.commit()

    spawn(upload_video_in_background, session_id)

    return "Upload started in background."

def upload_video_in_background(session_id):
    logger.info("Started video upload")
    with app.app_context():
        result_url = None
        webhook_url = "https://hook.eu1.make.com/meg7mme3bo1fgmuin2e2be1e1uayn5i9"
        try:
            db_row = db.session.query(OAuthStateDb).filter_by(session_id_db=session_id).first()
            if not db_row:
                logger.error("Session ID not found in database")
                return
            
            creds = get_service_account_credentials()
            creds.refresh(Request())
            drive_service = build("drive", "v3", credentials=creds)
            youtube_service = build("youtube", "v3", credentials=creds)
            content_owner = os.environ.get("CONTENT_OWNER")

            file_url = db_row.file_url_db
            logger.info(f"Downloading file from {file_url}")
            file_id = extract_file_id_from_url(file_url)
            request_drive = drive_service.files().get_media(fileId=file_id)
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".mp4")

            downloader = MediaIoBaseDownload(temp_file, request_drive)
            done = False
            previous = -5
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    percent = int(status.progress() * 100)
                    socketio.emit("download_progress", {"percent": percent}, room=session_id)
                    logger.info(f"Download progress: {percent}%")

                    if db_row.recordid_db and percent // 5 > previous // 5:
                        previous = percent
                        try:
                            response = requests.post(webhook_url, params={
                                "recordid": db_row.recordid_db,
                                "status": 200,
                                "type": "download",
                                "value": percent / 100
                            })
                            if response.status_code == 200:
                                logger.info(f"Webhook called at {percent}% (download)")
                            else:
                                logger.warning(f"Webhook error {response.status_code} at {percent}% (download)")
                        except Exception as e:
                            logger.warning(f"Error calling webhook (download): {e}")
            
            temp_file.close()
            fh = open(temp_file.name, "rb")

            status_dict = {
                "privacyStatus": db_row.status_db,
                "selfDeclaredMadeForKids": False,
                "notifySubscribers": str(db_row.notify_db).lower() in ["true", "1", "yes"]
            }

            if db_row.publishAt_db:
                status_dict["publishAt"] = db_row.publishAt_db

            body = {
                "snippet": {
                    "title": db_row.title_db,
                    "description": db_row.description_db,
                    "tags": db_row.tags_db.split(",") if db_row.tags_db else [],
                    "categoryId": db_row.category_db or "22",
                },
                "status": status_dict
            }

            media = MediaIoBaseUpload(fh, mimetype="video/mp4", resumable=True)

            if db_row.channelID_db:
                if db_row.channelID_db.startswith("UC") and len(db_row.channelID_db) == 24:
                    insert_request = youtube_service.videos().insert(
                        part="snippet,status",
                        body=body,
                        media_body=media,
                        onBehalfOfContentOwner=content_owner,
                        onBehalfOfContentOwnerChannel=db_row.channelID_db
                    )
                else:
                    logger.error("Invalid channelID sent in final_upload")
                    return f"ChannelID error : Wrong value, please verify the channelID : {db_row.channelID_db}"
            else:
                logger.info("No channelID sent in final_upload, publishing to main channel")
                insert_request = youtube_service.videos().insert(
                    part="snippet,status",
                    body=body,
                    media_body=media
                )

            response = None
            previous = -5
            logged = []
            
            while response is None:
                status, response = insert_request.next_chunk()
                if status:
                    percent = int(status.progress() * 100)
                    if percent not in logged :
                        socketio.emit("upload_progress", {"percent": percent}, room=session_id)
                        logger.info(f"Upload progress: {percent}%")
                        logged.append(percent)
                    if db_row.recordid_db and percent // 5 > previous // 5:
                        previous = percent
                        try:
                            response2 = requests.post(webhook_url, params={
                                "recordid": db_row.recordid_db,
                                "status": 200,
                                "type": "upload",
                                "value": percent / 100
                            })
                            if response2.status_code == 200:
                                logger.info(f"Webhook called at {percent}% (upload)")
                            else:
                                logger.warning(f"Webhook error {response2.status_code} at {percent}% (upload)")
                        except Exception as e:
                            logger.warning(f"Error calling webhook (upload): {e}")

            video_id = response.get("id")
            result_url = f"https://youtube.com/watch?v={video_id}"
            logger.info(f"Upload complete, video ID: {video_id}")

            #uploaded = load_uploaded_videos()
            #uploaded[video_id] = {
            #     "title": db_row.title_db,
            #     "session_id": session_id,
            #     "recordid": db_row.recordid_db,
            #     "channelID": db_row.channelID_db,
            #     "file_url": db_row.file_url_db,
            #     "upload_time": time.time()
            # }
            #save_uploaded_videos(uploaded)

            db_row.status_db = "uploaded"
            db.session.commit()

            if db_row.recordid_db:
                try:
                    response3 = requests.post(webhook_url, params={
                        "recordid": db_row.recordid_db,
                        "status": 200,
                        "yturl": result_url,
                        "type": "upload",
                        "value": 1
                    })
                    if response3.status_code == 200:
                        logger.info("Webhook successfully called for final result.")
                    else:
                        logger.warning(f"Webhook error: {response3.status_code} (final)")
                except Exception as e:
                    logger.warning(f"Webhook call error (final): {e}")

            socketio.emit("upload_complete", {"video_id": video_id, "yt_url": result_url, "session_id": session_id})

        except HttpError as e:
            logger.error(f"An HTTP error occurred: {e}")
            db_row.status_db = "error"
            db.session.commit()
            socketio.emit("upload_error", {"error": str(e), "session_id": session_id})
        except Exception as e:
            logger.exception("Unexpected error during upload.")
            db_row.status_db = "error"
            db.session.commit()
            socketio.emit("upload_error", {"error": str(e), "session_id": session_id})
        finally:
            if result_url:
                upload_results[session_id] = {"status": "success", "yt_url": result_url}
            else:
                upload_results[session_id] = {"status": "error", "error_message": "Upload failed"}
            try:
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.remove(temp_file.name)
            except Exception as e:
                logger.warning(f"Failed to delete temp file: {e}")


def extract_file_id_from_url(url):
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    if "id" in query:
        return query["id"][0]
    parts = url.split("/")
    if "d" in parts:
        idx = parts.index("d")
        return parts[idx + 1]
    return url

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))
