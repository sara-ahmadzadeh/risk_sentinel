import json
from datetime import datetime, timedelta
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="Risk Sentinel API",
    description="Real-time smart home monitoring API",
    version="1.0.0"
)

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://admin:password123@localhost:5432/risk_sentinel")
engine = create_engine(DATABASE_URL)

# ============ HEALTH CHECK ============
@app.get("/")
def root():
    return {
        "status": "healthy",
        "service": "Risk Sentinel API",
        "version": "1.0.0"
    }

@app.get("/health")
def health():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except:
        return {"status": "unhealthy", "database": "disconnected"}

# ============ ALERTS ENDPOINTS ============
@app.get("/alerts/recent")
def get_recent_alerts(limit: int = Query(20, ge=1, le=100)):
    """Get the most recent alerts"""
    with engine.connect() as conn:
        query = text("""
            SELECT id, device_id, alert_type, severity, details, created_at
            FROM alerts
            ORDER BY created_at DESC
            LIMIT :limit
        """)
        results = conn.execute(query, {"limit": limit})
        
        alerts = []
        for row in results:
            alerts.append({
                "id": row.id,
                "device_id": row.device_id,
                "alert_type": row.alert_type,
                "severity": row.severity,
                "details": row.details if row.details else {},  # FIXED: row.details is already a dict
                "created_at": row.created_at.isoformat()
            })
        return {
            "alerts": alerts,
            "count": len(alerts),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/alerts/count")
def get_alert_count(severity: str = None):
    """Get total alert count, optionally filtered by severity"""
    with engine.connect() as conn:
        if severity:
            query = text("""
                SELECT severity, COUNT(*) as count
                FROM alerts
                WHERE severity = :severity
                GROUP BY severity
            """)
            results = conn.execute(query, {"severity": severity})
        else:
            query = text("""
                SELECT severity, COUNT(*) as count
                FROM alerts
                GROUP BY severity
            """)
            results = conn.execute(query)
        
        stats = {row.severity: row.count for row in results}
        return {
            "severity_counts": stats,
            "total": sum(stats.values()),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/alerts/history")
def get_alert_history(hours: int = Query(24, ge=1, le=168)):
    """Get alert frequency by hour for the last N hours"""
    time_threshold = datetime.now() - timedelta(hours=hours)
    
    with engine.connect() as conn:
        query = text("""
            SELECT 
                DATE_TRUNC('hour', created_at) as hour,
                COUNT(*) as alert_count,
                severity
            FROM alerts
            WHERE created_at > :time_threshold
            GROUP BY DATE_TRUNC('hour', created_at), severity
            ORDER BY hour ASC
        """)
        results = conn.execute(query, {"time_threshold": time_threshold})
        
        history = []
        for row in results:
            history.append({
                "hour": row.hour.isoformat(),
                "alert_count": row.alert_count,
                "severity": row.severity
            })
        return {
            "history": history,
            "hours": hours,
            "timestamp": datetime.now().isoformat()
        }

# ============ SENSOR DATA ENDPOINTS ============
@app.get("/sensors/latest")
def get_latest_readings():
    """Get the latest reading from each device"""
    with engine.connect() as conn:
        query = text("""
            SELECT DISTINCT ON (device_id) 
                device_id, 
                payload, 
                received_at
            FROM raw_events
            ORDER BY device_id, received_at DESC
        """)
        results = conn.execute(query)
        
        readings = []
        for row in results:
            payload = row.payload if isinstance(row.payload, dict) else json.loads(row.payload)
            readings.append({
                "device_id": row.device_id,
                "payload": payload,
                "received_at": row.received_at.isoformat()
            })
        return {
            "readings": readings,
            "count": len(readings),
            "timestamp": datetime.now().isoformat()
        }

@app.get("/sensors/rooms")
def get_room_summary():
    """Get summary of all rooms with latest readings"""
    with engine.connect() as conn:
        query = text("""
            SELECT DISTINCT ON (device_id) 
                device_id, 
                payload, 
                received_at
            FROM raw_events
            ORDER BY device_id, received_at DESC
        """)
        results = conn.execute(query)
        
        rooms = {}
        for row in results:
            payload = row.payload if isinstance(row.payload, dict) else json.loads(row.payload)
            room = payload.get("room", "unknown")
            device_id = row.device_id
            
            if room not in rooms:
                rooms[room] = {
                    "room": room,
                    "devices": {},
                    "last_update": row.received_at.isoformat()
                }
            
            if "temperature_f" in payload:
                rooms[room]["temperature"] = payload["temperature_f"]
            if "humidity" in payload:
                rooms[room]["humidity"] = payload["humidity"]
            if "motion_detected" in payload:
                rooms[room]["motion"] = payload["motion_detected"]
            if "smoke_level" in payload:
                rooms[room]["smoke"] = payload["smoke_level"]
            if "co2_ppm" in payload:
                rooms[room]["co2"] = payload["co2_ppm"]
            if "is_on" in payload:
                rooms[room]["lights_on"] = payload["is_on"]
            if "is_open" in payload:
                if "window" in device_id:
                    rooms[room]["window_open"] = payload["is_open"]
                elif "door" in device_id:
                    rooms[room]["door_open"] = payload["is_open"]
        
        return {
            "rooms": list(rooms.values()),
            "count": len(rooms),
            "timestamp": datetime.now().isoformat()
        }

# ============ STATISTICS ENDPOINTS ============
@app.get("/stats/summary")
def get_summary_stats():
    """Get overall pipeline statistics"""
    one_hour_ago = datetime.now() - timedelta(hours=1)
    
    with engine.connect() as conn:
        query1 = text("""
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT device_id) as unique_devices
            FROM raw_events
            WHERE received_at > :one_hour_ago
        """)
        result1 = conn.execute(query1, {"one_hour_ago": one_hour_ago})
        row1 = result1.fetchone()
        
        query2 = text("""
            SELECT 
                COUNT(*) as total_alerts,
                COUNT(DISTINCT severity) as unique_severities
            FROM alerts
            WHERE created_at > :one_hour_ago
        """)
        result2 = conn.execute(query2, {"one_hour_ago": one_hour_ago})
        row2 = result2.fetchone()
        
        return {
            "events_last_hour": row1.total_events,
            "unique_devices": row1.unique_devices,
            "alerts_last_hour": row2.total_alerts,
            "unique_severities": row2.unique_severities,
            "timestamp": datetime.now().isoformat()
        }

@app.get("/stats/devices")
def get_device_stats():
    """Get device statistics by type"""
    with engine.connect() as conn:
        query = text("""
            SELECT 
                SPLIT_PART(device_id, '_', -1) as device_type,
                COUNT(*) as event_count,
                COUNT(DISTINCT device_id) as unique_devices
            FROM raw_events
            GROUP BY SPLIT_PART(device_id, '_', -1)
            ORDER BY device_type
        """)
        results = conn.execute(query)
        
        stats = []
        for row in results:
            stats.append({
                "device_type": row.device_type,
                "event_count": row.event_count,
                "unique_devices": row.unique_devices
            })
        return {
            "device_stats": stats,
            "timestamp": datetime.now().isoformat()
        }

# ============ RUN SERVER ============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)