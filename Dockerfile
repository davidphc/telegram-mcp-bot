FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py .

# /data is where Railway mounts the persistent volume.
# This RUN line is a local-dev fallback only — Railway's volume overlays it.
RUN mkdir -p /data

ENV PORT=8080
ENV DB_PATH=/data/messages.db

CMD ["python", "server.py"]
