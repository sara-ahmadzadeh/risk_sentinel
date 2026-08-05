import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_events")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")

engine = create_engine(DATABASE_URL)

# Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='ingester-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
print(f"🔄 Kafka Ingester Started (Source: kafka_ingester)")
print(f"   Listening on topic: {KAFKA_TOPIC}")
print("   Press Ctrl+C to stop\n")

message_count = 0
last_report = datetime.now()

with engine.connect() as conn:
    for message in consumer:
        payload = message.value
        device_id = payload.get("device_id", "unknown")
        topic = payload.get("_meta", {}).get("mqtt_topic", "unknown")
        
        # Insert into PostgreSQL with source
        insert_query = text("""
            INSERT INTO raw_events (device_id, topic, payload, received_at, source)
            VALUES (:device_id, :topic, :payload, :received_at, :source)
        """)
        conn.execute(
            insert_query,
            {
                "device_id": device_id,
                "topic": topic,
                "payload": json.dumps(payload),
                "received_at": datetime.now(timezone.utc),
                "source": "kafka_ingester"
            }
        )
        conn.commit()
        
        message_count += 1
        if (datetime.now() - last_report).seconds >= 30:
            print(f"[📊] Ingested {message_count} messages from Kafka")
            message_count = 0
            last_report = datetime.now()