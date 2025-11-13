# Data Engineering Docker Stack

A fully containerized **Data Engineering Platform** built with Docker Compose.  
It includes a complete analytics pipeline — ingestion, orchestration, storage, visualization, and a Python (Flask) microservices layer for data access and dashboards.

---

## Stack Overview

| Layer | Component | Description |
|-------|------------|-------------|
| **DataBase** | PostgreSQL | Primary OLTP database for app & Airflow metadata |
| **DataWarehouse**  | ClickHouse | Analytical OLAP database for fast queries |
| **Storage** | MinIO | S3-compatible object storage for datasets |
| **Messaging** | Apache Kafka 4.1.0 | Event streaming backbone |
| **Orchestration** | Apache Airflow | Workflow scheduling and ETL orchestration |
| **Computation** | Apache Spark | Distributed data processing |
| **Caching / Queues** | Redis | Caching, pub/sub, and Airflow broker |
| **Visualization** | Grafana | Dashboards and metrics visualization |
| **App Layer** | Flask app microservices | Custom micro-APIs and dashboards for CRUD and analytics |
| **Gateway** | Nginx | API reverse proxy and routing between services |

---

## Architecture

    +---------------------+         +-----------------------+
    |    API Gateway      | <-----> |   Flask Microservices  |
    |   (Nginx reverse)   |         | (Auth, CRUD, Dashboard)|
    +----------+----------+         +-----------+------------+
               |                                |
               v                                |
         +-----+------+                         |
         |   PostgreSQL | <---- Airflow ---------+
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

