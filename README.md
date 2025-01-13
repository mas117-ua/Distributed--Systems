# 🚕 EasyCab

## Distributed Autonomous Taxi Fleet Management System

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/)
[![Kafka](https://img.shields.io/badge/Kafka-3.0%2B-orange)](https://kafka.apache.org/)
[![Flask](https://img.shields.io/badge/Flask-2.0%2B-green)](https://flask.palletsprojects.com/)
[![University](https://img.shields.io/badge/University%20of%20Alicante-2024%2F2025-red)](https://www.ua.es/)

## 📋 Overview

EasyCab is a distributed system designed to manage a fleet of autonomous taxis in a simulated city environment. Developed as part of the Distributed Systems course at the University of Alicante (2024/2025), it showcases real-world applications of distributed computing concepts including:

- Secure socket communications
- REST APIs
- SSL/TLS encryption
- Event streaming through Kafka

## ✨ Key Features

### 🚗 Real-time Fleet Management
- Autonomous taxi routing and control
- Live position tracking on a 20x20 grid map
- Automatic assignment of taxis to clients
- Dynamic route calculation with wraparound map edges

### 🔐 Advanced Security Implementation
- SSL/TLS encryption for all communications
- Token-based authentication system
- AES-GCM encryption for sensitive data
- Comprehensive audit logging

### 🌦️ Environmental Monitoring
- Real-time weather monitoring via OpenWeather API
- Automatic taxi recall in severe weather conditions
- Simulated sensor system for obstacle detection

### 🏗️ Distributed Architecture
- Event-driven communication using Apache Kafka
- REST API for system monitoring and control
- Modular component design
- Robust error handling and recovery

## 🏛️ System Architecture

### Core Components

| Component | Description |
|-----------|-------------|
| **EC_Central** | Central control system managing the taxi fleet |
| **EC_CTC** | Traffic management module interfacing with OpenWeather API |
| **EC_Registry** | Handles taxi registration and authentication |
| **Front-end** | Web interface for system visualization |

### Taxi Components

| Component | Description |
|-----------|-------------|
| **EC_DE** | Core logic for each autonomous taxi |
| **EC_S** | Simulates vehicle sensors and environmental monitoring |

### Supporting Systems

- **Database**: Stores system state and taxi information
- **Kafka**: Handles event streaming between components
- **Security Layer**: Manages authentication and encrypted communications

## 🛠️ Technologies Used

### Core Stack
- **Python 3.8+** - Main development language
- **Apache Kafka** - Event streaming with SSL
- **Flask** - REST API implementation
- **PyCrypto** - Encryption operations

### Libraries
- `confluent-kafka` - Kafka integration
- `requests` - HTTP communications
- `flask-cors` - API cross-origin support
- `termios` - Sensor simulation

### Security
- OpenSSL - Certificate management
- AES-GCM - Payload encryption
- Token-based authentication

## 🚀 Setup and Deployment

### Prerequisites
- Python 3.8 or higher
- Apache Kafka with SSL enabled
- OpenSSL for certificate generation
- OpenWeather API key
- 3+ networked computers for distributed deployment

### Component Setup

1. **Central System**
```
python EC_Central.py <port> <kafka_broker_ip:port>
```

2. **Digital Engine**
```
python EC_DE.py <central_ip> <central_port> <sensor_ip:port> <kafka_broker_ip:port> <taxi_id>
```

3. **Sensor System**
```
python EC_S.py <ip:port>
```

4. **API Server**
```
python API_Central.py
```

## 🔒 Security Features

### Channel Security
- SSL/TLS encryption for all network communication
- Certificate-based authentication
- Secure Kafka communication

### Authentication
- Token-based system for taxi authentication
- Temporary token storage
- Automatic token expiration on disconnect

### Data Protection
- AES-GCM encryption for sensitive payloads
- Secure credential management
- Protected configuration files

### Audit System
- Comprehensive event logging
- Authentication tracking
- System state changes recording


Developed as part of the Distributed Systems Course at University of Alicante (2024/2025)
