"""
ML Model Training Script for Risk Sentinel
Trains an Isolation Forest model on historical sensor data
"""

import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

MODEL_DIR = "ml/models"
os.makedirs(MODEL_DIR, exist_ok=True)

class MLAnomalyTrainer:
    def __init__(self):
        self.df = None
        self.features = []
        self.scaler = StandardScaler()
        self.model = None
        
    def fetch_training_data(self, hours=24):
        """Fetch historical sensor data for training"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        query = text("""
            SELECT 
                device_id,
                payload,
                received_at
            FROM raw_events
            WHERE received_at > :cutoff_time
                AND (payload->>'temperature_f') IS NOT NULL
                OR (payload->>'humidity') IS NOT NULL
                OR (payload->>'motion_detected') IS NOT NULL
            ORDER BY received_at ASC
        """)
        
        with engine.connect() as conn:
            result = conn.execute(query, {"cutoff_time": cutoff_time})
            rows = result.fetchall()
            
        data = []
        for row in rows:
            payload = row.payload if isinstance(row.payload, dict) else json.loads(row.payload)
            data.append({
                "device_id": row.device_id,
                "received_at": row.received_at,
                **payload
            })
        
        self.df = pd.DataFrame(data)
        print(f"✅ Fetched {len(self.df)} records for training")
        return self.df
    
    def engineer_features(self):
        """Create features for the ML model"""
        df = self.df.copy()
        
        # Extract numeric features with fallbacks
        df["temp"] = df["temperature_f"].astype(float) if "temperature_f" in df.columns else np.nan
        df["humidity"] = df["humidity"].astype(float) if "humidity" in df.columns else np.nan
        df["smoke"] = df["smoke_level"].astype(float) if "smoke_level" in df.columns else 0.0
        df["co2"] = df["co2_ppm"].astype(float) if "co2_ppm" in df.columns else 400.0
        
        # Handle motion: fill NaN with 0, then convert to int
        if "motion_detected" in df.columns:
            df["motion"] = df["motion_detected"].fillna(0).astype(int)
        else:
            df["motion"] = 0
        
        # Extract room from device_id
        df["room"] = df["device_id"].apply(lambda x: '_'.join(x.split('_')[:-1]))
        
        # Time features
        df["hour"] = df["received_at"].dt.hour
        df["day_of_week"] = df["received_at"].dt.dayofweek
        df["is_weekend"] = df["day_of_week"].apply(lambda x: 1 if x >= 5 else 0)
        
        # Room type encoding (one-hot)
        room_dummies = pd.get_dummies(df["room"], prefix="room")
        df = pd.concat([df, room_dummies], axis=1)
        
        # Define features for the model
        self.features = [
            "temp", "humidity", "smoke", "co2", "motion",
            "hour", "day_of_week", "is_weekend"
        ] + [col for col in df.columns if col.startswith("room_")]
        
        # Drop rows with NaN in critical features (temp and humidity are essential)
        df = df.dropna(subset=["temp", "humidity"])
        
        # For remaining rows, fill any other NaN with 0 (safe for smoke, co2, motion)
        df[self.features] = df[self.features].fillna(0)
        
        self.df = df
        print(f"✅ Engineered {len(self.features)} features")
        print(f"   Features: {self.features[:5]}...")
        print(f"   Final dataset size: {len(self.df)} rows")
        return self.df
    
    def train_model(self, contamination=0.05, random_state=42):
        """Train Isolation Forest model"""
        X = self.df[self.features].values
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Split for validation
        X_train, X_val = train_test_split(X_scaled, test_size=0.2, random_state=random_state)
        
        # Train Isolation Forest
        self.model = IsolationForest(
            contamination=contamination,
            random_state=random_state,
            n_estimators=100,
            max_samples='auto'
        )
        self.model.fit(X_train)
        
        # Validate on validation set
        val_predictions = self.model.predict(X_val)
        anomaly_rate = (val_predictions == -1).sum() / len(val_predictions)
        
        print(f"✅ Model trained successfully!")
        print(f"   Training samples: {len(X_train)}")
        print(f"   Validation samples: {len(X_val)}")
        print(f"   Anomaly rate on validation: {anomaly_rate:.2%}")
        print(f"   Expected anomaly rate: {contamination:.2%}")
        
        return self.model
    
    def save_model(self, filename="isolation_forest.joblib"):
        """Save model and scaler to disk"""
        model_path = os.path.join(MODEL_DIR, filename)
        scaler_path = os.path.join(MODEL_DIR, "scaler.joblib")
        features_path = os.path.join(MODEL_DIR, "features.joblib")
        
        joblib.dump(self.model, model_path)
        joblib.dump(self.scaler, scaler_path)
        joblib.dump(self.features, features_path)
        
        print(f"✅ Model saved to {model_path}")
        print(f"✅ Scaler saved to {scaler_path}")
        print(f"✅ Features saved to {features_path}")
        
        return model_path
    
    def run(self, hours=24):
        """Full training pipeline"""
        print("🚀 Starting ML Model Training")
        print("=" * 50)
        
        # Step 1: Fetch data
        self.fetch_training_data(hours=hours)
        
        # Step 2: Engineer features
        self.engineer_features()
        
        # Step 3: Train model
        self.train_model()
        
        # Step 4: Save model
        self.save_model()
        
        print("=" * 50)
        print("🎉 Training Complete!")
        print(f"   Model can detect anomalies from {len(self.features)} features")
        print(f"   Use ml/predict.py to run real-time predictions")

if __name__ == "__main__":
    trainer = MLAnomalyTrainer()
    trainer.run(hours=24)