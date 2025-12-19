FROM apache/spark:3.5.3-python3

USER root

RUN pip install --no-cache-dir \
    clickhouse-driver \
    psycopg2-binary

USER spark
WORKDIR /opt/spark-app
