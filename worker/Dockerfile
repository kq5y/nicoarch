FROM python:3.12-slim

WORKDIR /app

RUN apt update && apt -y upgrade && apt -y install ffmpeg

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "worker.py"]