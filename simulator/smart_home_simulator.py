import json
import random
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# Configuration
MQTT_BROKER = "test.mosquitto.org"  # Free public broker
MQTT_PORT = 1883
TOPIC_BASE = "risk_sentinel"

# Device configurations
DEVICES = {
    "thermostat_livingroom": {
        "topic": f"{TOPIC_BASE}/sensors/temperature",
        "temperature_range": (68, 74),
        "humidity_range": (40, 50),
        "device_type": "temperature"
    },
    "thermostat_kitchen": {
        "topic": f"{TOPIC_BASE}/sensors/temperature",
        "temperature_range": (66, 72),
        "humidity_range": (45, 55),
        "device_type": "temperature"
    },
    "motion_sensor": {
        "topic": f"{TOPIC_BASE}/security/motion",
        "motion_probability": 0.1,  # 10% chance of motion
        "device_type": "motion"
    },
    "door_sensor": {
        "topic": f"{TOPIC_BASE}/security/door",
        "open_probability": 0.05,  # 5% chance of being open
        "device_type": "door"
    },
    "smoke_detector": {
        "topic": f"{TOPIC_BASE}/safety/smoke",
        "smoke_range": (0.0, 0.5),  # 0-0.5 is normal
        "anomaly_chance": 0.02,  # 2% chance of a spike
        "device_type": "smoke"
    }
}

class SmartHomeSimulator:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.running = True
        
    def generate_reading(self, device_name, config):
        """Generate a realistic sensor reading"""
        timestamp = datetime.now(timezone.utc).isoformat()
        base_payload = {
            "device_id": device_name,
            "timestamp": timestamp
        }
        
        # Get device type from config
        device_type = config.get("device_type", "")
        
        if device_type == "temperature":
            # Temperature sensor
            temp = random.uniform(*config["temperature_range"])
            humidity = random.uniform(*config["humidity_range"])
            # Occasionally inject an anomaly
            if random.random() < 0.03:  # 3% anomaly rate
                temp += random.uniform(20, 40)  # Big spike
            payload = {
                **base_payload,
                "temperature": round(temp, 1),
                "humidity": round(humidity, 1),
                "battery": round(random.uniform(85, 100), 1)
            }
            
        elif device_type == "motion":
            motion = random.random() < config["motion_probability"]
            payload = {
                **base_payload,
                "motion_detected": motion
            }
            
        elif device_type == "door":
            is_open = random.random() < config["open_probability"]
            payload = {
                **base_payload,
                "status": "open" if is_open else "closed"
            }
            
        elif device_type == "smoke":
            smoke_level = random.uniform(*config["smoke_range"])
            # Inject smoke anomaly
            if random.random() < config["anomaly_chance"]:
                smoke_level = random.uniform(2.0, 5.0)  # Dangerous level
            payload = {
                **base_payload,
                "smoke_level": round(smoke_level, 3)
            }
        else:
            # Fallback for unknown device types
            payload = {
                **base_payload,
                "status": "unknown"
            }
            
        return payload
    
    def publish_reading(self, device_name, config):
        """Publish a single reading to MQTT"""
        payload = self.generate_reading(device_name, config)
        topic = config["topic"]
        
        self.client.publish(topic, json.dumps(payload))
        print(f"[PUBLISH] {topic} → {payload}")
        
    def run(self, interval_seconds=2):
        """Main loop - publish readings every N seconds"""
        print("🏠 Smart Home Simulator Started")
        print(f"Publishing to broker: {MQTT_BROKER}")
        print("Press Ctrl+C to stop\n")
        
        try:
            while self.running:
                for device_name, config in DEVICES.items():
                    self.publish_reading(device_name, config)
                    time.sleep(0.2)  # Small delay between devices
                
                time.sleep(interval_seconds - 1.0)  # Adjust for total cycle time
                
        except KeyboardInterrupt:
            print("\n🛑 Simulator stopped")
            self.running = False
            self.client.disconnect()

if __name__ == "__main__":
    simulator = SmartHomeSimulator()
    simulator.run()