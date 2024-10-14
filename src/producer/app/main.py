import json
import os

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_ADVERTISED_LISTENERS", "kafka:9093,"),
    'client.id': 'producer'
}

producer = Producer(conf)


class Message(BaseModel):
    text: str


@app.post("/publish/")
def publish_message(message: Message):
    producer.produce('test_topic', key='key', value=json.dumps(message.model_dump()), callback=delivery_report)
    producer.flush()
    return {"status": "Message sent", "message": message.text}


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
