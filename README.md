# 📊 Superset + PostgreSQL Dashboard Project

This project sets up [Apache Superset](https://superset.apache.org/) using Docker Compose and connects it to a PostgreSQL database to create interactive KPI dashboards and visualizations.

---

## 📁 Project: CDR_Project

This repository is focused on processing and analyzing **Call Detail Records (CDR)** using PostgreSQL, and visualizing KPIs through Superset dashboards.

---

## 🧰 Tech Stack

- Docker & Docker Compose  
- Apache Superset  
- PostgreSQL (16)  
- Redis (for caching)  
- Nginx (reverse proxy)  
- pgAdmin (optional, for DB inspection)

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/CDR_Project.git
cd CDR_Project


### 2. To set up Superset use docker
'''bash
docker-compose up -d

### 3. Login to Superset
Use http://localhost:8088
