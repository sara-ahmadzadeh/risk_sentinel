import json
import os
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# MQTT Configuration
MQTT_BROKER = os.getenv("MQTT_BROKER", "broker.emqx.io")
MQTT_PORT = 1883

# Kafka Configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_events")

# Topics to subscribe to
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

class MQTTToKafkaBridge:
    def __init__(self):
        # MQTT Client
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        
        # Kafka Producer
        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("   Make sure Kafka is running: docker-compose up -d")
            raise
        
        # Stats
        self.message_count = 0
        self.last_report = time.time()
        
    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        print(f"✅ Connected to MQTT broker (code: {reason_code})")
        for topic in TOPICS:
            client.subscribe(topic)
            print(f"   📡 Subscribed: {topic}")
            
    def on_mqtt_message(self, client, userdata, msg):
        """Forward MQTT message to Kafka"""
        try:
            # Parse MQTT message
            payload = json.loads(msg.payload.decode('utf-8'))
            
            # Enrich with metadata
            enriched = {
                **payload,
                "_meta": {
                    "mqtt_topic": msg.topic,
                    "received_at": datetime.now(timezone.utc).isoformat()
                }
            }
            
            # Send to Kafka
            future = self.kafka_producer.send(KAFKA_TOPIC, value=enriched)
            self.kafka_producer.flush()
            
            self.message_count += 1
            if time.time() - self.last_report >= 30:
                print(f"[📊] Forwarded {self.message_count} messages to Kafka")
                self.message_count = 0
                self.last_report = time.time()
                
        except Exception as e:
            print(f"[ERROR] Failed to forward message: {e}")
            
    def run(self):
        print("🔄 MQTT → Kafka Bridge Started")
        print(f"   MQTT Broker: {MQTT_BROKER}")
        print(f"   Kafka Topic: {KAFKA_TOPIC}")
        print("   Press Ctrl+C to stop\n")
        
        self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.mqtt_client.loop_forever()

if __name__ == "__main__":
    bridge = MQTTToKafkaBridge()
    bridge.run()