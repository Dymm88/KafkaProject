import json
import os

from confluent_kafka import Producer
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_ADVERTISED_LISTENERS", "localhost:9092,"),
    'client.id': 'producer'
}

producer = Producer(conf)


class Message(BaseModel):
    text: str


@app.post("/publish/")
async def publish_message(message: Message):
    producer.produce('test_topic', key='key', value=json.dumps(message.model_dump()))
    producer.flush()
    return {"status": "Message sent", "message": message.text}
