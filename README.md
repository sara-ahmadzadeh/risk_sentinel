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

