import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# ============ CONFIGURATION ============
API_URL = "http://localhost:8000"

st.set_page_config(
    page_title="Risk Sentinel - Smart Home Monitor",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============ SIDEBAR ============
with st.sidebar:
    st.title("🏠 Risk Sentinel")
    st.caption("Smart Home Monitoring System")
    st.divider()
    
    # System Status
    st.subheader("📊 System Status")
    try:
        response = requests.get(f"{API_URL}/stats/summary", timeout=3)
        if response.status_code == 200:
            stats = response.json()
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Events (1hr)", stats["events_last_hour"])
            with col2:
                st.metric("Alerts (1hr)", stats["alerts_last_hour"])
            st.caption(f"🟢 Online • {stats['unique_devices']} devices")
        else:
            st.error("⚠️ API not reachable")
            st.caption("🔴 Offline")
    except:
        st.error("⚠️ API not reachable")
        st.caption("🔴 Offline")
    
    st.divider()
    
    # Alert Stats
    st.subheader("🚨 Alert Severity")
    try:
        response = requests.get(f"{API_URL}/alerts/count", timeout=2)
        if response.status_code == 200:
            data = response.json()
            severity_colors = {
                "CRITICAL": "🔴",
                "HIGH": "🟠", 
                "MEDIUM": "🟡",
                "LOW": "🟢"
            }
            for severity, count in data["severity_counts"].items():
                st.write(f"{severity_colors.get(severity, '⚪')} **{severity}**: {count}")
    except:
        st.write("Unable to fetch alert stats")
    
    st.divider()
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("🔄 Refresh Data"):
        st.rerun()

# ============ MAIN PAGE ============
st.title("🏠 Real-Time Smart Home Monitor")

# ============ KPI ROW ============
col1, col2, col3, col4 = st.columns(4)

try:
    response = requests.get(f"{API_URL}/stats/summary", timeout=2)
    if response.status_code == 200:
        stats = response.json()
        with col1:
            st.metric("📡 Events/Hour", stats["events_last_hour"], delta=None)
        with col2:
            st.metric("🚨 Alerts/Hour", stats["alerts_last_hour"], delta=None)
        with col3:
            st.metric("📊 Unique Devices", stats["unique_devices"], delta=None)
        with col4:
            st.metric("🏷️ Alert Types", stats["unique_severities"], delta=None)
    else:
        for col in [col1, col2, col3, col4]:
            with col:
                st.error("No data")
except:
    for col in [col1, col2, col3, col4]:
        with col:
            st.error("API Error")

st.divider()

# ============ TWO-COLUMN LAYOUT ============
left_col, right_col = st.columns([3, 2])

# ============ LEFT COLUMN: ROOMS OVERVIEW ============
with left_col:
    st.subheader("🏘️ Room Overview")
    
    try:
        response = requests.get(f"{API_URL}/sensors/rooms", timeout=2)
        if response.status_code == 200:
            data = response.json()
            rooms = data["rooms"]
            
            # Display rooms in a grid
            room_cols = st.columns(3)
            for idx, room in enumerate(rooms):
                with room_cols[idx % 3]:
                    with st.container(border=True):
                        room_name = room["room"].replace("_", " ").title()
                        st.markdown(f"**{room_name}**")
                        
                        # Temperature
                        if "temperature" in room:
                            temp = room["temperature"]
                            st.metric("🌡️ Temperature", f"{temp}°F")
                        
                        # Humidity
                        if "humidity" in room:
                            hum = room["humidity"]
                            st.metric("💧 Humidity", f"{hum}%")
                        
                        # Motion
                        if "motion" in room:
                            motion = "🔴 Active" if room["motion"] else "🟢 Clear"
                            st.write(f"**Motion:** {motion}")
                        
                        # Smoke
                        if "smoke" in room and room["smoke"] > 0.1:
                            st.warning(f"💨 Smoke: {room['smoke']}")
                        
                        # Window
                        if "window_open" in room:
                            status = "🔓 Open" if room["window_open"] else "🔒 Closed"
                            st.write(f"**Window:** {status}")
                        
                        # Lights
                        if "lights_on" in room:
                            status = "💡 On" if room["lights_on"] else "🔌 Off"
                            st.write(f"**Lights:** {status}")
        else:
            st.error("Failed to fetch room data")
    except:
        st.error("API Error - Is the FastAPI server running?")

# ============ RIGHT COLUMN: ALERTS ============
with right_col:
    st.subheader("🚨 Recent Alerts")
    
    try:
        response = requests.get(f"{API_URL}/alerts/recent?limit=15", timeout=2)
        if response.status_code == 200:
            data = response.json()
            alerts = data["alerts"]
            
            if not alerts:
                st.info("✅ No recent alerts - System is quiet")
            else:
                for alert in alerts[:15]:
                    severity = alert["severity"]
                    severity_colors = {
                        "CRITICAL": {"bg": "#ffcccc", "icon": "🔴"},
                        "HIGH": {"bg": "#ffe0cc", "icon": "🟠"},
                        "MEDIUM": {"bg": "#fff3cc", "icon": "🟡"},
                        "LOW": {"bg": "#e8f5e9", "icon": "🟢"}
                    }
                    colors = severity_colors.get(severity, {"bg": "#f5f5f5", "icon": "⚪"})
                    
                    # Extract reason from details
                    reason = alert["details"].get("reason", "No details")
                    if len(reason) > 60:
                        reason = reason[:60] + "..."
                    
                    st.markdown(f"""
                        <div style="padding: 8px; border-radius: 4px; margin-bottom: 6px; 
                                    background-color: {colors['bg']}; border-left: 4px solid {'#ff4444' if severity in ['CRITICAL','HIGH'] else '#ffaa00' if severity == 'MEDIUM' else '#4caf50'};">
                            <b>{colors['icon']} {severity}</b> 
                            <span style="font-size: 0.9rem;">{alert['device_id'].replace('_', ' ')}</span><br/>
                            <span style="font-size: 0.85rem; color: #555;">{reason}</span><br/>
                            <span style="font-size: 0.75rem; color: #888;">{alert['created_at'][:19].replace('T', ' ')}</span>
                        </div>
                    """, unsafe_allow_html=True)
        else:
            st.error("Failed to fetch alerts")
    except:
        st.error("API Error - Is the FastAPI server running?")

# ============ BOTTOM SECTION: CHARTS ============
st.divider()

# ============ ALERT HISTORY CHART ============
st.subheader("📈 Alert History (Last 24 Hours)")

try:
    response = requests.get(f"{API_URL}/alerts/history?hours=24", timeout=2)
    if response.status_code == 200:
        data = response.json()
        history = data["history"]
        
        if history:
            # Convert to DataFrame
            df = pd.DataFrame(history)
            df["hour"] = pd.to_datetime(df["hour"])
            
            # Create bar chart
            fig = px.bar(
                df,
                x="hour",
                y="alert_count",
                color="severity",
                title="Alert Frequency by Hour",
                labels={"hour": "Time", "alert_count": "Number of Alerts"},
                color_discrete_map={
                    "CRITICAL": "#ff4444",
                    "HIGH": "#ff8800",
                    "MEDIUM": "#ffcc00",
                    "LOW": "#4caf50"
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No alert history available yet")
    else:
        st.error("Failed to fetch alert history")
except:
    st.error("API Error")

# ============ AUTO-REFRESH ============
st.divider()
st.caption("🔄 Dashboard auto-refreshes every 10 seconds")

# Auto-refresh logic
if 'last_refresh' not in st.session_state:
    st.session_state.last_refresh = time.time()

if time.time() - st.session_state.last_refresh > 10:
    st.session_state.last_refresh = time.time()
    st.rerun()