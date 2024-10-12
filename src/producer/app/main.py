import json
import os

import psycopg2
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
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


def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "test"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host="postgres",
        port=5432
    )
    return conn


@app.post("/publish/")
def publish_message(message: Message):
    create_topic_if_not_exists('test_topic')
    conn = get_db_connection()
    curs = conn.cursor()
    curs.execute("INSERT INTO messages (text) VALUES (%s)", (message.text,))
    producer.produce('test_topic', key='key', value=json.dumps(message.model_dump()), callback=delivery_report)
    producer.flush()
    conn.commit()
    curs.close()
    conn.close()
    return {"status": "Message sent", "message": message.text}


def create_topic_if_not_exists(topic_name):
    admin_client = AdminClient({'bootstrap.servers': os.getenv("KAFKA_ADVERTISED_LISTENERS", "kafka:9093")})
    
    topic_list = admin_client.list_topics().topics
    
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        
        for topic, f in fs.items():
            try:
                f.result()
                print(f"Топик '{topic}' создан.")
            except Exception as e:
                print(f"Ошибка при создании топика '{topic}': {e}")
    else:
        print(f"Топик '{topic_name}' уже существует.")


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
