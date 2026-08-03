import json
import os
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883

TOPICS = [
    "risk_sentinel/sensors/temperature",
    "risk_sentinel/security/motion",
    "risk_sentinel/security/windows",
    "risk_sentinel/security/doors",
    "risk_sentinel/safety/smoke",
    "risk_sentinel/energy/lights",
    "risk_sentinel/sensors/humidity",
    "risk_sentinel/environment/co2"
]

class MQTTIngester:
    def __init__(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.count = 0
        self.last_report = time.time()
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        print(f"✅ Connected to MQTT")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"   Subscribed: {topic}")
            
    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            device_id = payload.get("device_id", "unknown")
            
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        INSERT INTO raw_events (device_id, topic, payload, received_at)
                        VALUES (:device_id, :topic, :payload, :received_at)
                    """),
                    {
                        "device_id": device_id,
                        "topic": msg.topic,
                        "payload": json.dumps(payload),
                        "received_at": datetime.now(timezone.utc),
                        "source": "mqtt_ingester"
                    }
                )
                conn.commit()
            
            self.count += 1
            if time.time() - self.last_report >= 30:
                print(f"[📊] Ingested {self.count} messages in 30s")
                self.count = 0
                self.last_report = time.time()
                
        except Exception as e:
            print(f"[ERROR] {e}")
            
    def run(self):
        print("🔄 Ingester started")
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_forever()

if __name__ == "__main__":
    import time
    MQTTIngester().run()