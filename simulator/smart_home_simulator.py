import json
import random
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
import math

MQTT_BROKER = "broker.emqx.io"
MQTT_PORT = 1883
TOPIC_BASE = "risk_sentinel"

class SmartHomeSimulator:
    def __init__(self):
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.running = True
        self.states = {"windows": {}, "doors": {}, "motion": {}}
        self.publish_count = 0
        self.last_report = time.time()
        
    def _get_room_temperature(self, room):
        hour = datetime.now().hour
        room_base = {
            "living_room": 72, "kitchen": 74, "master_bedroom": 70,
            "guest_bedroom": 68, "bathroom": 72, "garage": 65,
            "basement": 62, "attic": 78, "office": 71, "dining_room": 71
        }.get(room, 72)
        
        if 6 <= hour <= 9:
            temp = room_base + 1
        elif 12 <= hour <= 15:
            temp = room_base + 3 if room in ["living_room", "kitchen"] else room_base + 1
        elif 22 <= hour or hour <= 5:
            temp = room_base - 2
        else:
            temp = room_base
            
        if self.states["windows"].get(room, False):
            outside_temp = 55 + (15 * math.sin((hour - 6) / 24 * 2 * math.pi))
            temp = temp + 0.3 * (outside_temp - temp)
            
        return round(temp + random.uniform(-1.5, 1.5), 1)
    
    def _get_room_occupancy(self, room, hour):
        if room in ["living_room", "kitchen", "dining_room"]:
            return 6 <= hour <= 23
        elif room == "master_bedroom":
            return 22 <= hour or hour <= 7
        elif room == "guest_bedroom":
            return random.random() < 0.1
        elif room == "garage":
            return 7 <= hour <= 9 or 16 <= hour <= 18
        elif room == "office":
            return 8 <= hour <= 18
        else:
            return random.random() < 0.3
    
    def generate_temperature_reading(self, room, timestamp):
        temp = self._get_room_temperature(room)
        humidity = 30 + (15 * math.sin((datetime.now().hour - 6) / 24 * 2 * math.pi)) + random.uniform(-5, 5)
        return {
            "device_id": f"{room}_thermostat",
            "room": room,
            "device_type": "thermostat",
            "timestamp": timestamp,
            "temperature_f": temp,
            "temperature_c": round((temp - 32) * 5/9, 1),
            "humidity": round(max(20, min(80, humidity)), 1),
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_motion_reading(self, room, timestamp):
        hour = datetime.now().hour
        occupied = self._get_room_occupancy(room, hour)
        prob = 0.3 if occupied else 0.05
        if self.states["motion"].get(room, 0) > time.time() - 10:
            prob = 0.7
        motion = random.random() < prob
        if motion:
            self.states["motion"][room] = time.time()
        return {
            "device_id": f"{room}_motion",
            "room": room,
            "device_type": "motion_sensor",
            "timestamp": timestamp,
            "motion_detected": motion,
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_window_reading(self, room, timestamp):
        hour = datetime.now().hour
        is_open = random.random() < (0.15 if 10 <= hour <= 18 else 0.02)
        self.states["windows"][room] = is_open
        return {
            "device_id": f"{room}_window",
            "room": room,
            "device_type": "window_sensor",
            "timestamp": timestamp,
            "is_open": is_open,
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_door_reading(self, room, door_type, timestamp):
        hour = datetime.now().hour
        if door_type == "front_door":
            is_open = (7 <= hour <= 9 and random.random() < 0.3) or (17 <= hour <= 19 and random.random() < 0.4)
        elif door_type == "back_door":
            is_open = random.random() < 0.05
        elif door_type == "garage_door":
            is_open = (7 <= hour <= 9 or 16 <= hour <= 18) and random.random() < 0.2
        else:
            is_open = random.random() < 0.2
        self.states["doors"][f"{room}_{door_type}"] = is_open
        return {
            "device_id": f"{room}_{door_type}",
            "room": room,
            "device_type": "door_sensor",
            "timestamp": timestamp,
            "is_open": is_open,
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_smoke_reading(self, room, timestamp):
        hour = datetime.now().hour
        baseline = 0.2 if room == "kitchen" and (6 <= hour <= 9 or 17 <= hour <= 20) else 0.02
        smoke = baseline + random.uniform(-0.01, 0.05)
        if random.random() < 0.002:
            smoke = random.uniform(1.5, 4.0)
        return {
            "device_id": f"{room}_smoke_detector",
            "room": room,
            "device_type": "smoke_detector",
            "timestamp": timestamp,
            "smoke_level": round(max(0, smoke), 3),
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_light_reading(self, room, timestamp):
        hour = datetime.now().hour
        occupied = self._get_room_occupancy(room, hour)
        is_dark = hour < 7 or hour > 18
        if is_dark and occupied:
            is_on = random.random() < 0.8
        elif is_dark:
            is_on = random.random() < 0.2
        else:
            is_on = random.random() < 0.1
        return {
            "device_id": f"{room}_light",
            "room": room,
            "device_type": "light_switch",
            "timestamp": timestamp,
            "is_on": is_on,
            "brightness": random.randint(0, 100) if is_on else 0
        }
    
    def generate_humidity_reading(self, room, timestamp):
        hour = datetime.now().hour
        if room == "bathroom":
            baseline = 60
            if (6 <= hour <= 8 or 19 <= hour <= 21) and random.random() < 0.4:
                baseline += random.uniform(20, 35)
        else:
            baseline = 40
        humidity = baseline + random.uniform(-10, 10)
        return {
            "device_id": f"{room}_humidity_sensor",
            "room": room,
            "device_type": "humidity_sensor",
            "timestamp": timestamp,
            "humidity": round(max(10, min(90, humidity)), 1),
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_co2_reading(self, room, timestamp):
        hour = datetime.now().hour
        occupied = self._get_room_occupancy(room, hour)
        co2 = 400 + (random.uniform(200, 800) if occupied else random.uniform(0, 100))
        return {
            "device_id": f"{room}_co2_sensor",
            "room": room,
            "device_type": "co2_sensor",
            "timestamp": timestamp,
            "co2_ppm": round(co2, 0),
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def run(self):
        rooms = ["living_room", "kitchen", "master_bedroom", "guest_bedroom",
                 "bathroom", "garage", "basement", "attic", "office", "dining_room"]
        
        sensors = []
        for room in rooms:
            sensors.append((room, self.generate_temperature_reading, "sensors/temperature", 30))
            sensors.append((room, self.generate_motion_reading, "security/motion", 2))
            sensors.append((room, self.generate_window_reading, "security/windows", 60))
            sensors.append((room, self.generate_smoke_reading, "safety/smoke", 30))
            sensors.append((room, self.generate_light_reading, "energy/lights", 15))
            sensors.append((room, self.generate_humidity_reading, "sensors/humidity", 60))
            sensors.append((room, self.generate_co2_reading, "environment/co2", 120))
        
        door_configs = [("living_room", "front_door"), ("back_porch", "back_door"),
                       ("garage", "garage_door"), ("kitchen", "internal_door"),
                       ("master_bedroom", "internal_door")]
        for room, door_type in door_configs:
            sensors.append((room, self.generate_door_reading, "security/doors", 30, door_type))
        sensors.append(("front_entrance", self.generate_door_reading, "security/doors", 10, "front_door"))
        
        last_publish = {s: 0 for s in sensors}
        
        print(f"🏠 Simulator started → {MQTT_BROKER}")
        
        try:
            while self.running:
                now = time.time()
                timestamp = datetime.now(timezone.utc).isoformat()
                
                for sensor in sensors:
                    if len(sensor) == 4:
                        room, gen_func, topic, interval = sensor
                        extra = None
                    else:
                        room, gen_func, topic, interval, extra = sensor
                    
                    if now - last_publish[sensor] >= interval:
                        last_publish[sensor] = now
                        payload = gen_func(room, extra, timestamp) if extra else gen_func(room, timestamp)
                        self.client.publish(f"{TOPIC_BASE}/{topic}", json.dumps(payload))
                        self.publish_count += 1
                
                if time.time() - self.last_report >= 30:
                    print(f"[📊] {self.publish_count} messages published in 30s")
                    self.publish_count = 0
                    self.last_report = time.time()
                
                time.sleep(0.5)
        except KeyboardInterrupt:
            print("\n🛑 Simulator stopped")

if __name__ == "__main__":
    SmartHomeSimulator().run()