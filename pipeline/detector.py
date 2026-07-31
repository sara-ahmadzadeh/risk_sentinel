import json
import os
import time
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import numpy as np

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

# Configuration
CHECK_INTERVAL = 5  # seconds
TIME_WINDOW_MINUTES = 10
STD_DEVIATION_THRESHOLD = 3.0

class AnomalyDetector:
    def __init__(self):
        self.last_check = datetime.now(timezone.utc)
        
    def detect_temperature_anomaly(self, device_id, current_temp):
        """Check if current temperature is a statistical anomaly"""
        time_window = datetime.now(timezone.utc) - timedelta(minutes=TIME_WINDOW_MINUTES)
        
        with engine.connect() as conn:
            query = text("""
                SELECT 
                    AVG((payload->>'temperature')::FLOAT) as avg_temp,
                    STDDEV((payload->>'temperature')::FLOAT) as stddev_temp,
                    COUNT(*) as sample_count
                FROM raw_events
                WHERE device_id = :device_id
                    AND received_at > :time_window
                    AND (payload->>'temperature') IS NOT NULL
            """)
            
            result = conn.execute(query, {"device_id": device_id, "time_window": time_window})
            row = result.fetchone()
            
            if row and row.sample_count > 10:  # Need enough data for statistics
                avg = row.avg_temp
                stddev = row.stddev_temp or 1.0
                
                if abs(current_temp - avg) > (STD_DEVIATION_THRESHOLD * stddev):
                    return {
                        "is_anomaly": True,
                        "severity": "HIGH" if abs(current_temp - avg) > (5 * stddev) else "MEDIUM",
                        "reason": f"Temperature {current_temp}°F is {abs(current_temp - avg)/stddev:.1f} stddev from mean ({avg:.1f}°F)",
                        "avg": avg,
                        "stddev": stddev
                    }
        return {"is_anomaly": False}
    
    def detect_motion_anomaly(self, device_id, motion_detected):
        """Check for unexpected motion (e.g., while house is 'away')"""
        if motion_detected:
            return {
                "is_anomaly": True,
                "severity": "MEDIUM",
                "reason": "Motion detected in secure area"
            }
        return {"is_anomaly": False}
    
    def detect_smoke_anomaly(self, device_id, smoke_level):
        """Check if smoke level exceeds safety threshold"""
        if smoke_level > 1.0:  # Threshold
            return {
                "is_anomaly": True,
                "severity": "CRITICAL",
                "reason": f"Smoke level {smoke_level} exceeds safety threshold (1.0)"
            }
        return {"is_anomaly": False}
    
    def process_new_events(self):
        """Fetch events since last check and run detection"""
        last_check = self.last_check
        self.last_check = datetime.now(timezone.utc)
        
        with engine.connect() as conn:
            # Get events since last check
            query = text("""
                SELECT id, device_id, topic, payload, received_at
                FROM raw_events
                WHERE received_at > :last_check
                ORDER BY received_at ASC
            """)
            results = conn.execute(query, {"last_check": last_check})
            events = results.fetchall()
            
            for event in events:
                # FIXED: payload is already a dict from PostgreSQL JSONB
                payload = event.payload  # <-- REMOVED json.loads()
                device_id = event.device_id
                alert = None
                
                # Route to appropriate detector based on device type
                if "temperature" in payload:
                    temp = payload["temperature"]
                    result = self.detect_temperature_anomaly(device_id, temp)
                    if result["is_anomaly"]:
                        alert = {
                            "device_id": device_id,
                            "alert_type": "temperature_spike",
                            "severity": result["severity"],
                            "details": {
                                "temperature": temp,
                                "reason": result["reason"],
                                "avg": result.get("avg"),
                                "stddev": result.get("stddev")
                            }
                        }
                            
                elif "motion_detected" in payload:
                    motion = payload["motion_detected"]
                    result = self.detect_motion_anomaly(device_id, motion)
                    if result["is_anomaly"]:
                        alert = {
                            "device_id": device_id,
                            "alert_type": "motion_alert",
                            "severity": result["severity"],
                            "details": {"reason": result["reason"]}
                        }
                        
                elif "smoke_level" in payload:
                    smoke = payload["smoke_level"]
                    result = self.detect_smoke_anomaly(device_id, smoke)
                    if result["is_anomaly"]:
                        alert = {
                            "device_id": device_id,
                            "alert_type": "smoke_detected",
                            "severity": result["severity"],
                            "details": {
                                "smoke_level": smoke,
                                "reason": result["reason"]
                            }
                        }
                
                # Insert alert if detected
                if alert:
                    insert_query = text("""
                        INSERT INTO alerts (device_id, alert_type, severity, details)
                        VALUES (:device_id, :alert_type, :severity, :details)
                    """)
                    conn.execute(
                        insert_query,
                        {
                            "device_id": alert["device_id"],
                            "alert_type": alert["alert_type"],
                            "severity": alert["severity"],
                            "details": json.dumps(alert["details"])
                        }
                    )
                    conn.commit()
                    print(f"[ALERT] {alert['severity']} | {alert['alert_type']} | {alert['device_id']}")
    
    def run(self):
        print("🎯 Anomaly Detector Started")
        print(f"Checking every {CHECK_INTERVAL} seconds...\n")
        
        try:
            while True:
                self.process_new_events()
                time.sleep(CHECK_INTERVAL)
        except KeyboardInterrupt:
            print("\n🛑 Detector stopped")

if __name__ == "__main__":
    detector = AnomalyDetector()
    detector.run()