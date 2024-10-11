import json
import os

import psycopg2
from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_ADVERTISED_LISTENERS", "localhost:9092,"),
    'client.id': 'producer'
}

producer = Producer(conf)


def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "test"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host="postgres",
        port=5432
    )
    return conn


class Message(BaseModel):
    text: str


@app.post("/publish/")
async def publish_message(message: Message):
    producer.produce('test_topic', key='key', value=json.dumps(message.model_dump()), callback=delivery_report)
    producer.flush()
    
    conn = get_db_connection()
    curs = conn.cursor()
    curs.execute("INSERT INTO messages (text) VALUES (%s)", (message.text,))
    conn.commit()
    curs.close()
    conn.close()
    return {"status": "Message sent", "message": message.text}


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
