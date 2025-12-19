# Data Engineering Docker Stack

A fully containerized **Data Engineering Platform** built with Docker Compose.  
It provides an end-to-end analytics/streaming pipeline: ingestion (CDC), messaging, processing (ETL), data warehousing, orchestration, and monitoring.

Repository:
- GitHub: https://github.com/A-Azarmi/dataStack
- Docker Hub: https://hub.docker.com/r/amirazarmi/data-stack
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

```
## Tests

CDC (PostgreSQL → Debezium → Kafka)
Connector config

Connector JSON:

connectors/debezium-northwind.json

Check connector status
curl -s http://localhost:8083/connectors | jq

curl -s http://localhost:8083/connectors/northwind-cdc-connector/status | jq


Expected:

connector.state = RUNNING

tasks[0].state = RUNNING

Check Kafka topics (inside docker network)
docker compose exec kafka bash -lc \
"kafka-topics --bootstrap-server kafka:9092 --list | grep -E '^northwind\.public\.' | sort"


You should see topics like:

northwind.public.customers

northwind.public.orders

northwind.public.order_details

northwind.public.products

...

Data Warehouse (ClickHouse)

ClickHouse is the Data Warehouse (DW) layer for analytics.

Database: northwind_dw

Customer dimension: northwind_dw.dim_customers

DDL scripts:

clickhouse/init/02_dim_customers.sql

Check tables
docker compose exec clickhouse clickhouse-client -q "SHOW DATABASES;"
docker compose exec clickhouse clickhouse-client -q "SHOW TABLES FROM northwind_dw;"

Spark Real-Time ETL

Main Spark streaming job:

spark/app/northwind_streaming.py

What it does (Current Implemented Business Rule)

Business Rule: Validate-before-write using Lookup on PostgreSQL

For every micro-batch read from Kafka topic northwind.public.customers:

Spark parses Debezium messages and extracts customer_id values.

Spark performs a lookup query on PostgreSQL to verify that each customer_id exists in public.customers.

Records with customer_id NOT found in PostgreSQL are dropped.

Only validated records are written to ClickHouse table northwind_dw.dim_customers (DW).

This rule guarantees:

✅ The rule always runs (it is inside foreachBatch before any write).

✅ Lookup happens before DW writes.

✅ ClickHouse is the authoritative DW output.

Note: The parser supports both Debezium formats:

{"payload":{"after":...,"op":...}}

{"after":...,"op":...}

Run Spark job

The Spark container stays alive; you run the job with spark-submit:

docker compose exec spark bash -lc \
"spark-submit \
 --conf spark.jars.ivy=/tmp/.ivy2 \
 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
 /opt/spark-app/northwind_streaming.py"

Test Plan (Executable Scenarios)

These tests are designed to be copy/paste runnable and produce clear PASS/FAIL outputs.

Pre-test: ensure DW table has required columns
docker compose exec clickhouse clickhouse-client -q \
"ALTER TABLE northwind_dw.dim_customers ADD COLUMN IF NOT EXISTS op String;"

docker compose exec clickhouse clickhouse-client -q \
"ALTER TABLE northwind_dw.dim_customers ADD COLUMN IF NOT EXISTS src_ts DateTime;"


(Optional) Reset table for clean testing:

docker compose exec clickhouse clickhouse-client -q "TRUNCATE TABLE northwind_dw.dim_customers;"
docker compose exec clickhouse clickhouse-client -q "SELECT count() FROM northwind_dw.dim_customers;"


(Optional) Reset Spark checkpoint for clean reprocessing:

docker compose exec spark bash -lc "rm -rf /tmp/spark-checkpoints/dim_customers || true"

Test Case 1: Valid Customer → Must be written to ClickHouse (DW)

Goal: Prove lookup passes and write occurs for a real customer existing in PostgreSQL.

Get a real customer_id:

docker compose exec postgres psql -U app_user -d northwind -t -c \
"SELECT customer_id FROM public.customers ORDER BY customer_id LIMIT 1;"


Update that customer to generate a CDC event:

docker compose exec postgres psql -U app_user -d northwind -c \
"UPDATE public.customers SET company_name = company_name || ' TEST_VALID' WHERE customer_id = 'ALFKI';"


Confirm DW output:

docker compose exec clickhouse clickhouse-client -q \
"SELECT count() FROM northwind_dw.dim_customers;"

docker compose exec clickhouse clickhouse-client -q \
"SELECT customer_id, company_name, op, src_ts
 FROM northwind_dw.dim_customers
 ORDER BY src_ts DESC
 LIMIT 5;"


Expected (PASS):

ClickHouse count becomes > 0

Latest rows include the updated customer_id

Test Case 2: Invalid Customer → Must be dropped BEFORE ClickHouse write

Goal: Prove lookup fails and record is not written to DW.

Inject an invalid Debezium-like message into Kafka (customer_id does not exist in Postgres):

docker compose exec kafka bash -lc '
cat <<EOF | kafka-console-producer --bootstrap-server kafka:9092 --topic northwind.public.customers
{"after":{"customer_id":"ZZZ_NOT_EXISTS","company_name":"Fake Co"},"op":"c"}
EOF'


Confirm it was NOT written to ClickHouse:

docker compose exec clickhouse clickhouse-client -q \
"SELECT count() FROM northwind_dw.dim_customers WHERE customer_id='ZZZ_NOT_EXISTS';"


Expected (PASS):

The query returns 0

Spark logs show lookup failure and skip write:

NOT in Postgres

SKIP write

Test Case 3: Mixed Valid + Invalid → Only Valid written to DW

Goal: Prove business rule filters within the same run.

Reset table:

docker compose exec clickhouse clickhouse-client -q "TRUNCATE TABLE northwind_dw.dim_customers;"


Inject invalid record (Test Case 2) then generate a valid event (Test Case 1).

Validate:

docker compose exec clickhouse clickhouse-client -q \
"SELECT count() FROM northwind_dw.dim_customers;"

docker compose exec clickhouse clickhouse-client -q \
"SELECT count() FROM northwind_dw.dim_customers WHERE customer_id='ZZZ_NOT_EXISTS';"


Expected (PASS):

Total count > 0

Invalid count = 0

Troubleshooting
Kafka Connect shows connector RUNNING but task FAILED

Check status:

curl -s http://localhost:8083/connectors/northwind-cdc-connector/status | jq


Common cause: Debezium publication mode mismatch in Postgres.
Fix: recreate connector with compatible publication.autocreate.mode or clean publication/slot (project scripts included).

ClickHouse query fails with missing columns (src_ts/op)

Add columns:

docker compose exec clickhouse clickhouse-client -q \
"ALTER TABLE northwind_dw.dim_customers ADD COLUMN IF NOT EXISTS op String;"
docker compose exec clickhouse clickhouse-client -q \
"ALTER TABLE northwind_dw.dim_customers ADD COLUMN IF NOT EXISTS src_ts DateTime;"

Spark shows empty batches

Ensure topics exist and have data

Reset Spark checkpoint:

docker compose exec spark bash -lc "rm -rf /tmp/spark-checkpoints/dim_customers || true"

Docker Image (Custom)

Spark image is built from:

spark/Dockerfile

Example build/push (maintainer):

docker build -t amirazarmi/data-stack:spark-3.5.3 -f spark/Dockerfile .
docker push amirazarmi/data-stack:spark-3.5.3

Next Steps (Planned)

Add Spark jobs for orders and order_details with advanced business rules:

inventory checks

discount limits

temporal rules (order/shipped dates)

cross-table validations

Add Airflow DAGs for running Spark jobs + data quality checks

Add Grafana dashboards for:

connector health

Kafka lag

Spark micro-batch metrics

ClickHouse ingestion rate
