import json
import os

import psycopg2
from confluent_kafka import Consumer, KafkaError


def consume_messages():
    consumer_config = {
        'bootstrap.servers': os.getenv("KAFKA_ADVERTISED_LISTENERS", "localhost:9092,"),
        'group.id': 'consumer_group',
        'auto.offset.reset': 'earliest',
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['test_topic'])
    
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "test"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host="postgres",
        port=5432
    )
    cursor = conn.cursor()
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            data = json.loads(msg.value().decode('utf-8'))
            cursor.execute("INSERT INTO messages (text) VALUES (%s)", (data['text'],))
            conn.commit()
    
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    consume_messages()
