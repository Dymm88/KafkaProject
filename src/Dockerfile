FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && pip install --no-cache-dir poetry

WORKDIR /app

COPY ./producer /app/producer
COPY ./consumer /app/consumer

RUN (cd /app/producer && poetry install --no-dev)
RUN (cd /app/consumer && poetry install --no-dev)

WORKDIR /app/producer/app

CMD ["poetry", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]