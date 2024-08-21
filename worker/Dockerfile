FROM python:3.12-bullseye

RUN apt update && apt -y upgrade && apt -y install ffmpeg

ADD https://github.com/tksnnx/nicoarch-worker.git /app
WORKDIR /app

ENV POETRY_HOME=/opt/poetry
RUN curl -sSL https://install.python-poetry.org | python3 - && \
    cd /usr/local/bin && \
    ln -s /opt/poetry/bin/poetry && \
    poetry config virtualenvs.create false

RUN poetry install

CMD ["poetry", "run", "python", "-u", "src/worker.py"]
