import json
import random
import time
from datetime import datetime, timezone, timedelta
import paho.mqtt.client as mqtt
import math

# Configuration
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
TOPIC_BASE = "risk_sentinel"

class SmartHomeSimulator:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.running = True
        
        # Track device states for relationship logic
        self.states = {
            "windows": {},  # track open/closed per room
            "doors": {},    # track open/closed per room
            "motion": {},   # track last motion time per room
        }
        
    def _get_room_temperature(self, room, base_temp=72):
        """Calculate temperature based on room, time, and window states"""
        hour = datetime.now().hour
        
        # Base temp varies by room
        room_base = {
            "living_room": 72,
            "kitchen": 74,
            "master_bedroom": 70,
            "guest_bedroom": 68,
            "bathroom": 72,
            "garage": 65,
            "basement": 62,
            "attic": 78,
            "office": 71,
            "dining_room": 71
        }.get(room, 72)
        
        # Time of day adjustment
        if 6 <= hour <= 9:
            temp = room_base + 1
        elif 12 <= hour <= 15:
            temp = room_base + 3 if room in ["living_room", "kitchen"] else room_base + 1
        elif 22 <= hour or hour <= 5:
            temp = room_base - 2
        else:
            temp = room_base
            
        # Window effect: if window is open, temp moves toward outside
        if self.states["windows"].get(room, False):
            outside_temp = 55 + (15 * math.sin((hour - 6) / 24 * 2 * math.pi))
            temp = temp + 0.3 * (outside_temp - temp)
            
        # Random variation
        temp += random.uniform(-1.5, 1.5)
        return round(temp, 1)
    
    def _get_room_occupancy(self, room, hour):
        """Determine if room is likely occupied based on time of day"""
        if room in ["living_room", "kitchen", "dining_room"]:
            return 6 <= hour <= 23
        elif room == "master_bedroom":
            return 22 <= hour or hour <= 7
        elif room == "guest_bedroom":
            return random.random() < 0.1
        elif room == "garage":
            return 7 <= hour <= 9 or 16 <= hour <= 18
        elif room == "basement":
            return random.random() < 0.2
        elif room == "attic":
            return False
        elif room == "office":
            return 8 <= hour <= 18
        else:
            return random.random() < 0.3
    
    def generate_temperature_reading(self, room, timestamp):
        """Generate temperature + humidity for a room"""
        temp = self._get_room_temperature(room)
        humidity = 30 + (15 * math.sin((datetime.now().hour - 6) / 24 * 2 * math.pi)) + random.uniform(-5, 5)
        humidity = round(max(20, min(80, humidity)), 1)
        
        return {
            "device_id": f"{room}_thermostat",
            "room": room,
            "device_type": "thermostat",
            "timestamp": timestamp,
            "temperature_f": temp,
            "temperature_c": round((temp - 32) * 5/9, 1),
            "humidity": humidity,
            "battery": round(random.uniform(80, 100), 1),
            "target_temp": round(temp + random.uniform(-2, 2), 1)
        }
    
    def generate_motion_reading(self, room, timestamp):
        """Generate motion detection for a room"""
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
        """Generate window sensor reading"""
        hour = datetime.now().hour
        
        # Windows open more during day, especially in summer
        is_open = random.random() < (0.15 if 10 <= hour <= 18 else 0.02)
        
        # Anomaly: window left open at night
        if 1 <= hour <= 4 and random.random() < 0.005:
            is_open = True
        
        # Track state for temperature calculations
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
        """Generate door sensor reading"""
        hour = datetime.now().hour
        
        # Door logic based on type
        if door_type == "front_door":
            # Opens at 8 AM and 6 PM (going to/from work)
            is_open = False
            if (7 <= hour <= 9 and random.random() < 0.3) or (17 <= hour <= 19 and random.random() < 0.4):
                is_open = True
            # Anomaly: door opens at 3 AM
            if 2 <= hour <= 4 and random.random() < 0.003:
                is_open = True
        elif door_type == "back_door":
            is_open = random.random() < 0.05
        elif door_type == "garage_door":
            # Opens when car comes/goes
            is_open = (7 <= hour <= 9 or 16 <= hour <= 18) and random.random() < 0.2
        else:  # internal doors
            is_open = random.random() < 0.2
        
        # Track state
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
        """Generate smoke detector reading"""
        hour = datetime.now().hour
        
        if room == "kitchen":
            baseline = 0.2 if 6 <= hour <= 9 or 17 <= hour <= 20 else 0.05
        else:
            baseline = 0.02
            
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
        """Generate light switch status"""
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
            "brightness": random.randint(0, 100) if is_on else 0,
            "power_watts": random.randint(0, 100) if is_on else 0
        }
    
    def generate_humidity_reading(self, room, timestamp):
        """Generate humidity sensor reading"""
        hour = datetime.now().hour
        
        if room == "bathroom":
            baseline = 60
            if (6 <= hour <= 8 or 19 <= hour <= 21) and random.random() < 0.4:
                baseline += random.uniform(20, 35)
        else:
            baseline = 40
            
        humidity = baseline + random.uniform(-10, 10)
        humidity = round(max(10, min(90, humidity)), 1)
        
        return {
            "device_id": f"{room}_humidity_sensor",
            "room": room,
            "device_type": "humidity_sensor",
            "timestamp": timestamp,
            "humidity": humidity,
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def generate_co2_reading(self, room, timestamp):
        """Generate CO2 sensor reading"""
        hour = datetime.now().hour
        occupied = self._get_room_occupancy(room, hour)
        
        baseline = 400
        if occupied:
            co2 = baseline + random.uniform(200, 800)
        else:
            co2 = baseline + random.uniform(0, 100)
            
        return {
            "device_id": f"{room}_co2_sensor",
            "room": room,
            "device_type": "co2_sensor",
            "timestamp": timestamp,
            "co2_ppm": round(co2, 0),
            "battery": round(random.uniform(80, 100), 1)
        }
    
    def run(self, interval_seconds=1):
        """Main loop"""
        print("🏠 Smart Home Simulator Started (Dense Sensor Network)")
        print(f"Publishing to broker: {MQTT_BROKER}")
        print("Press Ctrl+C to stop\n")
        
        # Define all rooms
        rooms = [
            "living_room", "kitchen", "master_bedroom", "guest_bedroom",
            "bathroom", "garage", "basement", "attic", "office", "dining_room"
        ]
        
        # Build sensor schedule: (room, generator_func, topic, interval_seconds, publish_probability, extra_args)
        sensors = []
        
        for room in rooms:
            # Each room has: thermostat, motion, window, smoke, light, humidity, CO2
            sensors.append((room, self.generate_temperature_reading, "sensors/temperature", 30, 1.0, None))
            sensors.append((room, self.generate_motion_reading, "security/motion", 2, 1.0, None))
            sensors.append((room, self.generate_window_reading, "security/windows", 60, 1.0, None))
            sensors.append((room, self.generate_smoke_reading, "safety/smoke", 30, 1.0, None))
            sensors.append((room, self.generate_light_reading, "energy/lights", 15, 0.8, None))
            sensors.append((room, self.generate_humidity_reading, "sensors/humidity", 60, 1.0, None))
            sensors.append((room, self.generate_co2_reading, "environment/co2", 120, 1.0, None))
        
        # Door sensors (specific rooms)
        door_configs = [
            ("living_room", "front_door"),
            ("back_porch", "back_door"),
            ("garage", "garage_door"),
            ("kitchen", "internal_door"),
            ("master_bedroom", "internal_door")
        ]
        for room, door_type in door_configs:
            sensors.append((room, self.generate_door_reading, "security/doors", 30, 1.0, door_type))
        
        # Extra: Special front entrance door
        sensors.append(("front_entrance", self.generate_door_reading, "security/doors", 10, 1.0, "front_door"))
        
        # Track last publish time for each sensor
        last_publish = {sensor: 0 for sensor in sensors}
        
        try:
            while self.running:
                now = time.time()
                timestamp = datetime.now(timezone.utc).isoformat()
                
                for sensor in sensors:
                    room, gen_func, topic, interval, prob, extra = sensor
                    
                    if now - last_publish[sensor] >= interval:
                        last_publish[sensor] = now
                        
                        if random.random() < prob:
                            if extra:
                                payload = gen_func(room, extra, timestamp)
                            else:
                                payload = gen_func(room, timestamp)
                            
                            full_topic = f"{TOPIC_BASE}/{topic}"
                            self.client.publish(full_topic, json.dumps(payload))
                            
                            # Print a subset to avoid spam
                            if random.random() < 0.03:
                                print(f"[PUBLISH] {full_topic} → {payload.get('device_id', 'unknown')}")
                
                time.sleep(interval_seconds)
                
        except KeyboardInterrupt:
            print("\n🛑 Simulator stopped")
            self.running = False
            self.client.disconnect()

if __name__ == "__main__":
    simulator = SmartHomeSimulator()
    simulator.run()