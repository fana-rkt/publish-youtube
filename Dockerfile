FROM python:3.10.10-slim

# Set working directory inside the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY . .

# Expose the port your app runs on
EXPOSE 8080

# Start the app with Gunicorn, binding to the dynamic port
CMD gunicorn app:app --worker-class eventlet -w 1 --bind 0.0.0.0:$PORT
