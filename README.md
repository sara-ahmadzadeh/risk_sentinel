# 🏠 Risk Sentinel — Real-Time Smart Home Monitoring

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)](https://www.postgresql.org/)
[![MQTT](https://img.shields.io/badge/MQTT-5.0+-660066.svg)](https://mqtt.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-009688.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.29+-FF4B4B.svg)](https://streamlit.io/)

## 📊 Overview

**Risk Sentinel** is an end-to-end operational intelligence pipeline that simulates a 76-sensor smart home, ingests real-time data via MQTT, detects anomalies using contextual rules, and exposes results through a REST API and live dashboard.

### What It Does

| Component | Description |
|-----------|-------------|
| **Simulator** | Generates realistic IoT data from 76 sensors across 10 rooms |
| **Ingester** | Subscribes to MQTT, writes raw data to PostgreSQL |
| **Detector** | Polls for new events, applies anomaly detection rules |
| **API** | FastAPI REST endpoints for alerts and sensor data |
| **Dashboard** | Streamlit UI showing live room status and alerts |

### Detected Anomalies

| Device Type | Detection Method |
|-------------|------------------|
| Temperature | 3-sigma statistical + rate-of-change |
| Motion | Time-of-day contextual rules |
| Smoke | Room-specific thresholds |
| Windows | Nighttime open + HVAC energy waste |
| Lights | Unoccupied rooms + daytime waste |
| Humidity | Room baselines + extreme levels |
| CO₂ | Occupancy-aware thresholds |
| Doors | Nighttime security + extended open |

---

## 🏗️ Architecture
Simulator ──MQTT──► Ingester ──► 
PostgreSQL (raw_events)
│
▼
Detector (polling 5s)
│
▼
PostgreSQL (alerts)
│
▼
FastAPI ──► Streamlit Dashboard


🚧 Future Enhancements
Kafka integration (real-time event streaming)

ML-powered detection (Isolation Forest)

Docker containerization

Cloud deployment (AWS/GCP)

Prometheus + Grafana monitoring