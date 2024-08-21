FROM python:3.12-slim

RUN apt update && apt -y upgrade && apt -y install ffmpeg

ADD https://github.com/tksnnx/nicoarch-worker.git /app
WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-u", "worker.py"]
