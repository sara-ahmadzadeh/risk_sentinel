import json
import os
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np
from collections import defaultdict

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

CHECK_INTERVAL = 5
TIME_WINDOW_MINUTES = 10
STD_DEVIATION_THRESHOLD = 3.0

class AnomalyDetector:
    def __init__(self):
        self.last_check = datetime.now(timezone.utc)
        self.device_states = defaultdict(dict)
        self.room_occupancy = defaultdict(bool)
        self.alert_count = 0
        self.last_report = time.time()
        
    def detect_temperature_anomaly(self, device_id, room, current_temp, timestamp):
        time_window = datetime.now(timezone.utc) - timedelta(minutes=TIME_WINDOW_MINUTES)
        with engine.connect() as conn:
            query = text("""
                SELECT AVG((payload->>'temperature_f')::FLOAT) as avg_temp,
                       STDDEV((payload->>'temperature_f')::FLOAT) as stddev_temp,
                       COUNT(*) as sample_count
                FROM raw_events
                WHERE device_id = :device_id
                    AND received_at > :time_window
                    AND (payload->>'temperature_f') IS NOT NULL
            """)
            result = conn.execute(query, {"device_id": device_id, "time_window": time_window})
            row = result.fetchone()
            if row and row.sample_count > 10:
                avg = row.avg_temp
                stddev = row.stddev_temp or 1.0
                z_score = abs(current_temp - avg) / stddev
                if z_score > STD_DEVIATION_THRESHOLD:
                    return {
                        "is_anomaly": True,
                        "severity": "HIGH" if z_score > 5 else "MEDIUM",
                        "reason": f"Temp {current_temp}°F is {z_score:.1f}σ from mean ({avg:.1f}°F)"
                    }
        return {"is_anomaly": False}
    
    def detect_motion_anomaly(self, device_id, room, motion_detected, timestamp):
        hour = datetime.now().hour
        room_type = device_id.replace('_motion', '')
        severity = "LOW"
        reason = "Motion detected"
        
        if hour < 6 or hour > 22:
            severity = "HIGH"
            reason = f"Motion at {hour}:00 in {room_type}"
        elif 9 <= hour <= 17 and "bedroom" in room_type:
            severity = "MEDIUM"
            reason = f"Unusual motion in {room_type} during work hours"
        elif 6 <= hour <= 8:
            severity = "LOW"
            reason = f"Morning activity in {room_type}"
        else:
            severity = "LOW"
            reason = f"Normal activity in {room_type}"
        
        if not self.room_occupancy.get(room, False) and motion_detected:
            severity = "MEDIUM" if severity == "LOW" else severity
            reason += " (room was unoccupied)"
        
        self.room_occupancy[room] = motion_detected
        
        return {
            "is_anomaly": True if severity in ["MEDIUM", "HIGH"] else False,
            "severity": severity,
            "reason": reason
        }
    
    def detect_smoke_anomaly(self, device_id, room, smoke_level, timestamp):
        room_type = device_id.replace('_smoke_detector', '')
        thresholds = {"kitchen": 0.8, "garage": 0.6, "basement": 0.5, "default": 0.3}
        threshold = thresholds.get(room_type, thresholds["default"])
        if smoke_level > 0.1:
            return {
                "is_anomaly": True,
                "severity": "CRITICAL" if smoke_level > threshold * 3 else "HIGH",
                "reason": f"Smoke {smoke_level} in {room_type} (threshold: {threshold})"
            }
        return {"is_anomaly": False}
    
    def detect_window_anomaly(self, device_id, room, is_open, timestamp):
        hour = datetime.now().hour
        if is_open:
            # Get current room temperature from JSON payload
            with engine.connect() as conn:
                query = text("""
                    SELECT (payload->>'temperature_f')::FLOAT as temp
                    FROM raw_events
                    WHERE device_id LIKE :room_prefix
                        AND (payload->>'temperature_f') IS NOT NULL
                        AND payload->>'device_type' = 'thermostat'
                    ORDER BY received_at DESC
                    LIMIT 1
                """)
                result = conn.execute(query, {"room_prefix": f"{room}%"})
                row = result.fetchone()
                room_temp = row.temp if row else 72
            
            if (hour < 6 or hour > 22) and datetime.now().month in [12, 1, 2]:
                return {
                    "is_anomaly": True,
                    "severity": "HIGH",
                    "reason": f"Window open at night in winter ({room_temp}°F inside)"
                }
            if room_temp > 75 or room_temp < 65:
                return {
                    "is_anomaly": True,
                    "severity": "MEDIUM",
                    "reason": f"Window open with HVAC running ({room_temp}°F)"
                }
        return {"is_anomaly": False}
    
    def detect_light_anomaly(self, device_id, room, is_on, timestamp):
        hour = datetime.now().hour
        room_occupied = self.room_occupancy.get(room, False)
        if is_on and not room_occupied:
            return {
                "is_anomaly": True,
                "severity": "MEDIUM",
                "reason": f"Lights on in {room} with no motion"
            }
        if is_on and 8 <= hour <= 17:
            return {
                "is_anomaly": True,
                "severity": "LOW",
                "reason": f"Lights on in {room} during daytime"
            }
        return {"is_anomaly": False}
    
    def detect_humidity_anomaly(self, device_id, room, humidity, timestamp):
        room_type = device_id.replace('_humidity_sensor', '')
        baselines = {"bathroom": 60, "kitchen": 50, "basement": 55, "default": 45}
        baseline = baselines.get(room_type, baselines["default"])
        if humidity > 80:
            return {
                "is_anomaly": True,
                "severity": "HIGH",
                "reason": f"Extreme humidity {humidity}% in {room_type}"
            }
        if humidity > baseline + 15:
            return {
                "is_anomaly": True,
                "severity": "MEDIUM",
                "reason": f"High humidity {humidity}% in {room_type} (baseline: {baseline}%)"
            }
        return {"is_anomaly": False}
    
    def detect_co2_anomaly(self, device_id, room, co2_level, timestamp):
        room_type = device_id.replace('_co2_sensor', '')
        room_occupied = self.room_occupancy.get(room, False)
        if co2_level > 1500 and room_occupied:
            return {
                "is_anomaly": True,
                "severity": "HIGH",
                "reason": f"High CO2 {co2_level}ppm in occupied {room_type}"
            }
        if co2_level > 3000:
            return {
                "is_anomaly": True,
                "severity": "CRITICAL",
                "reason": f"Critical CO2 {co2_level}ppm in {room_type}"
            }
        return {"is_anomaly": False}
    
    def detect_door_anomaly(self, device_id, room, is_open, timestamp):
        hour = datetime.now().hour
        door_type = device_id.split('_')[-1]
        if "front" in door_type and (hour < 6 or hour > 22) and is_open:
            return {
                "is_anomaly": True,
                "severity": "CRITICAL",
                "reason": f"{door_type} open at {hour}:00"
            }
        return {"is_anomaly": False}
    
    def process_new_events(self):
        last_check = self.last_check
        self.last_check = datetime.now(timezone.utc)
        
        with engine.connect() as conn:
            query = text("""
                SELECT id, device_id, topic, payload, received_at
                FROM raw_events
                WHERE received_at > :last_check
                ORDER BY received_at ASC
            """)
            results = conn.execute(query, {"last_check": last_check})
            events = results.fetchall()
            
            for event in events:
                payload = event.payload
                device_id = event.device_id
                room = '_'.join(device_id.split('_')[:-1])
                result = None
                alert_type = None
                
                if "thermostat" in device_id and "temperature_f" in payload:
                    result = self.detect_temperature_anomaly(device_id, room, payload["temperature_f"], event.received_at)
                    alert_type = "temperature"
                elif "motion" in device_id and "motion_detected" in payload:
                    result = self.detect_motion_anomaly(device_id, room, payload["motion_detected"], event.received_at)
                    alert_type = "motion"
                elif "smoke" in device_id and "smoke_level" in payload:
                    result = self.detect_smoke_anomaly(device_id, room, payload["smoke_level"], event.received_at)
                    alert_type = "smoke"
                elif "window" in device_id and "is_open" in payload:
                    result = self.detect_window_anomaly(device_id, room, payload["is_open"], event.received_at)
                    alert_type = "window"
                elif "light" in device_id and "is_on" in payload:
                    result = self.detect_light_anomaly(device_id, room, payload["is_on"], event.received_at)
                    alert_type = "light"
                elif "humidity" in device_id and "humidity" in payload:
                    result = self.detect_humidity_anomaly(device_id, room, payload["humidity"], event.received_at)
                    alert_type = "humidity"
                elif "co2" in device_id and "co2_ppm" in payload:
                    result = self.detect_co2_anomaly(device_id, room, payload["co2_ppm"], event.received_at)
                    alert_type = "co2"
                elif "door" in device_id and "is_open" in payload:
                    result = self.detect_door_anomaly(device_id, room, payload["is_open"], event.received_at)
                    alert_type = "door"
                
                if result and result.get("is_anomaly"):
                    conn.execute(
                        text("""
                            INSERT INTO alerts (device_id, alert_type, severity, details)
                            VALUES (:device_id, :alert_type, :severity, :details)
                        """),
                        {
                            "device_id": device_id,
                            "alert_type": alert_type,
                            "severity": result["severity"],
                            "details": json.dumps({"reason": result["reason"]})
                        }
                    )
                    conn.commit()
                    self.alert_count += 1
                    print(f"[🚨] {result['severity']} | {device_id} | {result['reason'][:60]}...")
    
    def run(self):
        print("🎯 Detector started (76 devices, contextual rules)")
        try:
            while True:
                self.process_new_events()
                if time.time() - self.last_report >= 60:
                    print(f"[📊] {self.alert_count} alerts generated in 60s")
                    self.alert_count = 0
                    self.last_report = time.time()
                time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            print("\n🛑 Detector stopped")

if __name__ == "__main__":
    AnomalyDetector().run()