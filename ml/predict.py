"""
Real-time ML Anomaly Detection
Consumes from Kafka, runs ML predictions, writes to alerts
"""

import os
import json
import time
import joblib
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import sys

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "raw_events")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")

engine = create_engine(DATABASE_URL)

MODEL_DIR = "ml/models"
MODEL_PATH = os.path.join(MODEL_DIR, "isolation_forest.joblib")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.joblib")
FEATURES_PATH = os.path.join(MODEL_DIR, "features.joblib")

class MLAnomalyDetector:
    def __init__(self):
        self.model = None
        self.scaler = None
        self.features = None
        self.buffer = []  # Buffer for accumulating samples
        self.buffer_size = 10  # Process in batches
        self.alert_count = 0
        self.last_report = time.time()
        
        # Load model
        self.load_model()
        
        # Setup Kafka consumer
        self.consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='ml-detector-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        print(f"✅ Connected to Kafka at {KAFKA_BROKER}")
        print(f"🎯 ML Detector Started (Source: ml_detector)")
        print(f"   Listening on topic: {KAFKA_TOPIC}")
        print("   Press Ctrl+C to stop\n")
        
    def load_model(self):
        """Load trained model from disk"""
        try:
            self.model = joblib.load(MODEL_PATH)
            self.scaler = joblib.load(SCALER_PATH)
            self.features = joblib.load(FEATURES_PATH)
            print(f"✅ Loaded ML model from {MODEL_PATH}")
            print(f"   Features: {len(self.features)}")
        except FileNotFoundError:
            print(f"❌ Model not found. Please run ml/train_model.py first.")
            sys.exit(1)
    
    def extract_features(self, payload):
        """Extract features from raw payload"""
        # Extract numeric values with defaults
        features = {
            "temp": float(payload.get("temperature_f", np.nan)),
            "humidity": float(payload.get("humidity", np.nan)),
            "smoke": float(payload.get("smoke_level", 0)),
            "co2": float(payload.get("co2_ppm", 400)),
            "motion": int(payload.get("motion_detected", 0))
        }
        
        # If critical features are missing, skip
        if pd.isna(features["temp"]) or pd.isna(features["humidity"]):
            return None
        
        # Time features
        now = datetime.now()
        features["hour"] = now.hour
        features["day_of_week"] = now.weekday()
        features["is_weekend"] = 1 if now.weekday() >= 5 else 0
        
        # Room from device_id
        device_id = payload.get("device_id", "unknown")
        room = '_'.join(device_id.split('_')[:-1])
        
        # Room one-hot encoding (match training)
        for feature in self.features:
            if feature.startswith("room_"):
                room_name = feature.replace("room_", "")
                features[feature] = 1 if room == room_name else 0
        
        return features
    
    def predict(self, features_dict):
        """Run ML prediction on a single sample"""
        # Convert to array in correct order
        X = np.array([features_dict[f] for f in self.features]).reshape(1, -1)
        
        # Scale
        X_scaled = self.scaler.transform(X)
        
        # Predict (-1 = anomaly, 1 = normal)
        prediction = self.model.predict(X_scaled)[0]
        score = self.model.decision_function(X_scaled)[0]
        
        return prediction, score
    
    def process_batch(self):
        """Process buffered samples"""
        if len(self.buffer) < self.buffer_size:
            return
        
        print(f"[ML] Processing batch of {len(self.buffer)} samples...")
        
        with engine.connect() as conn:
            for sample in self.buffer:
                device_id = sample["device_id"]
                features = sample["features"]
                raw_payload = sample["payload"]
                
                try:
                    prediction, score = self.predict(features)
                    
                    if prediction == -1:  # Anomaly
                        # Determine severity based on score
                        if score < -0.5:
                            severity = "CRITICAL"
                        elif score < -0.3:
                            severity = "HIGH"
                        elif score < -0.1:
                            severity = "MEDIUM"
                        else:
                            severity = "LOW"
                        
                        # Insert alert
                        conn.execute(
                            text("""
                                INSERT INTO alerts (device_id, alert_type, severity, details, source)
                                VALUES (:device_id, :alert_type, :severity, :details, :source)
                            """),
                            {
                                "device_id": device_id,
                                "alert_type": "ml_anomaly",
                                "severity": severity,
                                "details": json.dumps({
                                    "reason": f"ML detected anomaly (score: {score:.2f})",
                                    "score": round(score, 3),
                                    "features": {k: round(v, 2) if isinstance(v, float) else v 
                                                for k, v in features.items() if not k.startswith("room_")}
                                }),
                                "source": "ml_detector"
                            }
                        )
                        conn.commit()
                        self.alert_count += 1
                        
                        print(f"[🚨] ML | {severity} | {device_id} | Score: {score:.2f}")
                        
                except Exception as e:
                    print(f"[ERROR] ML prediction failed for {device_id}: {e}")
            
            # Clear buffer
            self.buffer = []
    
    def run(self):
        """Main loop - consume from Kafka and process"""
        with engine.connect() as conn:
            for message in self.consumer:
                payload = message.value
                device_id = payload.get("device_id", "unknown")
                
                # Extract features
                features = self.extract_features(payload)
                if features is None:
                    continue  # Skip incomplete data
                
                # Add to buffer
                self.buffer.append({
                    "device_id": device_id,
                    "features": features,
                    "payload": payload
                })
                
                # Process batch
                if len(self.buffer) >= self.buffer_size:
                    self.process_batch()
                
                # Periodic status
                if time.time() - self.last_report >= 60:
                    print(f"[📊] ML Detector: {self.alert_count} alerts in 60s")
                    self.alert_count = 0
                    self.last_report = time.time()

if __name__ == "__main__":
    detector = MLAnomalyDetector()
    detector.run()