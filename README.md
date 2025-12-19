# Data Engineering Docker Stack

A fully containerized **Data Engineering Platform** built with Docker Compose.  
It provides an end-to-end analytics/streaming pipeline: ingestion (CDC), messaging, processing (ETL), data warehousing, orchestration, and monitoring.

Repository:
- GitHub: https://github.com/A-Azarmi/dataStack
- Docker Hub: https://hub.docker.com/r/amirazarmi/data-stack

    +---------------------+         +-----------------------+
    |    API Gateway      | <-----> |   Flask Microservices  |
    |   (Nginx reverse)   |         | (Auth, CRUD, Dashboard)|
    +----------+----------+         +-----------+------------+
               |                                |
               v                                |
         +-----+------+                         |
         |   PostgreSQL | <---- Airflow --------+
         +-----+------+                         |
               |                                v
               |                        +-------+--------+
               |                        |   ClickHouse    |
               |                        +-----------------+
               |
               v
    +----------+------------+
    |        Kafka          |
    +----------+------------+
               |
               v
      +--------+---------+
      |      Spark       |
      +------------------+
               |
               v
     +---------+----------+
     |       MinIO        |
     +--------------------+
               |
               v
       +-------+--------+
       |     Grafana     |
       +-----------------+

---

## Stack Overview

| Layer | Component | Description |
|-------|------------|-------------|
| **DataBase** | PostgreSQL | Primary OLTP database (Northwind) |
| **DataWarehouse**  | ClickHouse | Analytical OLAP database (DW) |
| **Storage** | MinIO | S3-compatible object storage (future use) |
| **Messaging** | Apache Kafka | Event streaming backbone |
| **CDC** | Debezium (Kafka Connect) | Change Data Capture from PostgreSQL into Kafka topics |
| **Orchestration** | Apache Airflow | Workflow scheduling and orchestration (baseline included) |
| **Computation** | Apache Spark | Real-time ETL with business rules |
| **Visualization** | Grafana | Dashboards and metrics visualization |
| **App Layer** | Flask app microservices | Placeholder for API validation & access layer |
| **Gateway** | Nginx | API reverse proxy (optional / future) |

---

## Architecture

![alt text](dataStackFlow.png "dataStackFlow")

---

## Project Structure

```text
.
├── airflow
│   ├── dags
│   ├── logs
│   └── plugins
├── clickhouse
│   └── init
│       ├── 01_order_wide.sql
│       └── 02_dim_customers.sql
├── connectors
│   └── debezium-northwind.json
├── docker-compose.yml
├── flask_app
├── grafana
│   └── provisioning
├── minio
│   └── data
├── postgres
│   └── init
│       ├── 01_northwind.sql
│       └── 02_replication_user.sql
├── spark
│   ├── app
│   │   └── northwind_streaming.py
│   └── Dockerfile
└── spark-app
