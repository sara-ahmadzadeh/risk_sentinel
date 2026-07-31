import json
import os
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# Database connection
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

# MQTT Configuration
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
TOPICS = [
    "risk_sentinel/sensors/temperature",
    "risk_sentinel/security/motion",
    "risk_sentinel/security/door",
    "risk_sentinel/safety/smoke"
]

class MQTTIngester:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        
    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT broker with result code: {rc}")
        # Subscribe to all topics
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"Subscribed to: {topic}")
            
    def on_message(self, client, userdata, msg):
        """Called when a message is received from MQTT"""
        try:
            # Parse the payload
            payload = json.loads(msg.payload.decode('utf-8'))
            device_id = payload.get("device_id", "unknown")
            
            # Insert into PostgreSQL
            with engine.connect() as conn:
                insert_query = text("""
                    INSERT INTO raw_events (device_id, topic, payload, received_at)
                    VALUES (:device_id, :topic, :payload, :received_at)
                """)
                conn.execute(
                    insert_query,
                    {
                        "device_id": device_id,
                        "topic": msg.topic,
                        "payload": json.dumps(payload),  # Store as JSON string
                        "received_at": datetime.now(timezone.utc)
                    }
                )
                conn.commit()
                
            print(f"[INGESTED] {msg.topic} → {device_id} at {datetime.now(timezone.utc).strftime('%H:%M:%S')}")
            
        except Exception as e:
            print(f"[ERROR] Failed to process message: {e}")
            
    def run(self):
        print("🔄 MQTT Ingester Started")
        print(f"Connecting to broker: {MQTT_BROKER}")
        print("Waiting for messages... (Press Ctrl+C to stop)\n")
        
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_forever()

if __name__ == "__main__":
    ingester = MQTTIngester()
    ingester.run()