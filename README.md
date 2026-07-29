# 🏠 Risk Sentinel - Real-Time Smart Home Monitoring

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-336791.svg)](https://www.postgresql.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-009688.svg)](https://fastapi.tiangolo.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.29+-FF4B4B.svg)](https://streamlit.io/)
[![MQTT](https://img.shields.io/badge/MQTT-5.0+-660066.svg)](https://mqtt.org/)

## 📊 Overview

**Risk Sentinel** is a real-time operational intelligence pipeline that simulates a smart home environment, ingests live sensor data via MQTT, detects anomalies using statistical methods, and exposes results through a REST API and live dashboard.

### 🎯 Key Features

- **Real-Time Data Ingestion**: MQTT-based streaming with simulated IoT devices
- **Anomaly Detection**: Statistical outlier detection (3-sigma) for temperature, motion, and smoke
- **Event-Driven Architecture**: Immediate processing as data arrives
- **REST API**: FastAPI endpoints for alerts and sensor data
- **Live Dashboard**: Streamlit-based real-time visualization
- **PostgreSQL Storage**: Time-series optimized schema with JSONB support


## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL 15+
- pip (Python package manager)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/risk-sentinel.git
cd risk-sentinel

2. **Set up virtual environment
3. **Install dependencies
4. **Configure PostgreSQL
5. **Set environment variables
6. **Run the pipeline

Component	Command
Simulator	python simulator/smart_home_simulator.py
Ingester	python pipeline/ingester.py
Detector	python pipeline/detector.py
API Server	python api/main.py
Dashboard	streamlit run dashboard/streamlit_app.py
Quick Start (All Components)
bash
# Terminal 1
python simulator/smart_home_simulator.py

# Terminal 2
python pipeline/ingester.py

# Terminal 3
python pipeline/detector.py

# Terminal 4
python api/main.py

# Terminal 5
streamlit run dashboard/streamlit_app.py
📊 API Endpoints
Endpoint	Method	Description
/alerts/recent	GET	Get recent alerts (paginated)
/sensors/latest	GET	Get latest readings per device
/stats/summary	GET	Get summary statistics
Interactive API Documentation: http://localhost:8000/docs

🧪 Example Dashboard
https://screenshots/dashboard.png

📈 Technical Highlights
Event-Driven: Real-time processing with MQTT push model

Statistical Detection: 3-sigma anomaly detection with rolling windows

JSONB Storage: Flexible schema for heterogeneous IoT data

Optimized Queries: Indexed time-series queries for fast lookups

Clean Architecture: Separation of concerns (ingestion, detection, API, UI)

🔧 Tech Stack
Layer	Technology
IoT Simulation	Python, MQTT (paho-mqtt)
Data Streaming	MQTT Protocol
Database	PostgreSQL with JSONB
Anomaly Detection	Statistical methods (NumPy)
API Layer	FastAPI
Dashboard	Streamlit
Language	Python 3.10+
🎓 What I Learned
Building event-driven data pipelines with MQTT

Real-time anomaly detection using statistical methods

Designing time-series optimized database schemas

Creating REST APIs with FastAPI

Building interactive dashboards with Streamlit

Handling streaming data with Python

🚀 Future Enhancements
Replace rule-based detection with ML models (Isolation Forest, LSTM)

Add Kafka for decoupled streaming

Implement Apache Airflow for pipeline orchestration

Deploy to cloud (AWS/Azure/GCP)

Add Prometheus + Grafana for monitoring

Implement authentication for API

Add more device types and anomaly scenarios