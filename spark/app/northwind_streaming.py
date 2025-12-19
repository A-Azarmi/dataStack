import os
from typing import Set, List, Iterable, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    get_json_object,
    from_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from clickhouse_driver import Client
import psycopg2

from pyspark.sql.functions import coalesce


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CUSTOMERS_TOPIC = os.getenv("CUSTOMERS_TOPIC", "northwind.public.customers")


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))

CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "northwind_dw")

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

CLICKHOUSE_CUSTOMERS_TABLE = os.getenv("CLICKHOUSE_CUSTOMERS_TABLE", "dim_customers")

CHECKPOINT_LOCATION_CUSTOMERS = os.getenv(
    "CHECKPOINT_LOCATION_CUSTOMERS",
    "/tmp/spark-checkpoints/dim_customers",
)


POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "northwind")
POSTGRES_USER = os.getenv("POSTGRES_USER", "app_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "app_password")

POSTGRES_CUSTOMERS_TABLE = os.getenv("POSTGRES_CUSTOMERS_TABLE", "public.customers")


CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("company_name", StringType(), False),
        StructField("contact_name", StringType(), True),
        StructField("contact_title", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("country", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("fax", StringType(), True),
    ]
)


def create_spark_session(app_name: str = "NorthwindCustomersStreaming") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_raw_stream(spark: SparkSession, topic: str) -> DataFrame:

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("kafka.request.timeout.ms", "60000")
        .option("kafka.session.timeout.ms", "45000")
        .option("kafka.metadata.max.age.ms", "60000")
        .option("failOnDataLoss", "false")
        .load()
    )

    return df.select(
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("src_ts"),
    )


def read_customers_raw_stream(spark: SparkSession) -> DataFrame:
    return read_kafka_raw_stream(spark, CUSTOMERS_TOPIC)


# def parse_customers_from_debezium(raw_df: DataFrame) -> DataFrame:

#     customers_raw = raw_df.select(
#         get_json_object(col("value"), "$.payload.after").alias("after_json"),
#         get_json_object(col("value"), "$.payload.op").alias("op"),
#         col("src_ts"),
#     )

#     customers_typed = (
#         customers_raw
#         .select(
#             from_json(col("after_json"), CUSTOMERS_SCHEMA).alias("c"),
#             col("op"),
#             col("src_ts"),
#         )
#         .select(
#             col("c.customer_id").alias("customer_id"),
#             col("c.company_name").alias("company_name"),
#             col("c.contact_name").alias("contact_name"),
#             col("c.contact_title").alias("contact_title"),
#             col("c.address").alias("address"),
#             col("c.city").alias("city"),
#             col("c.region").alias("region"),
#             col("c.postal_code").alias("postal_code"),
#             col("c.country").alias("country"),
#             col("c.phone").alias("phone"),
#             col("c.fax").alias("fax"),
#             col("op"),
#             col("src_ts"),
#         )
#     )

#     customers_typed = customers_typed.filter(col("customer_id").isNotNull())

#     return customers_typed



def parse_customers_from_debezium(raw_df: DataFrame) -> DataFrame:
    """
    Supports both Debezium message formats:
      A) {"payload": {"after": {...}, "op": "c/u/d", ...}}
      B) {"after": {...}, "op": "c/u/d", ...}
    """

    customers_raw = raw_df.select(
        # try payload.after first, fallback to after
        coalesce(
            get_json_object(col("value"), "$.payload.after"),
            get_json_object(col("value"), "$.after")
        ).alias("after_json"),

        # try payload.op first, fallback to op
        coalesce(
            get_json_object(col("value"), "$.payload.op"),
            get_json_object(col("value"), "$.op")
        ).alias("op"),

        col("src_ts")
    )

    customers_typed = (
        customers_raw
        .select(
            from_json(col("after_json"), CUSTOMERS_SCHEMA).alias("c"),
            col("op"),
            col("src_ts")
        )
        .select(
            col("c.customer_id").alias("customer_id"),
            col("c.company_name").alias("company_name"),
            col("c.contact_name").alias("contact_name"),
            col("c.contact_title").alias("contact_title"),
            col("c.address").alias("address"),
            col("c.city").alias("city"),
            col("c.region").alias("region"),
            col("c.postal_code").alias("postal_code"),
            col("c.country").alias("country"),
            col("c.phone").alias("phone"),
            col("c.fax").alias("fax"),
            col("op"),
            col("src_ts")
        )
    )

    return customers_typed.filter(col("customer_id").isNotNull())


def get_clickhouse_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        database=CLICKHOUSE_DB,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )


def _safe_str(value) -> str:
    if value is None:
        return ""
    return str(value)


def _get_postgres_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def fetch_existing_ids_subset(table_fqn: str, id_col: str, ids: Iterable[Any]) -> Set[str]:

    ids_list = [i for i in ids if i is not None]
    if not ids_list:
        return set()

    conn = _get_postgres_connection()
    try:
        with conn.cursor() as cur:
            sql = f"SELECT {id_col} FROM {table_fqn} WHERE {id_col} = ANY(%s)"
            cur.execute(sql, (ids_list,))
            rows = cur.fetchall()

        existing: Set[str] = set()
        for (v,) in rows:
            if v is None:
                continue
            existing.add(str(v).strip())
        return existing
    finally:
        conn.close()


def validate_customers_batch(batch_df: DataFrame, batch_id: int) -> DataFrame:

    batch_ids_rows = batch_df.select("customer_id").distinct().collect()
    batch_ids: Set[str] = set()

    for row in batch_ids_rows:
        cid = row["customer_id"]
        if cid is None:
            continue
        batch_ids.add(str(cid).strip())

    if not batch_ids:
        print(f"[validate] batch {batch_id}: no customer_id in batch; SKIP write")
        return batch_df.limit(0)

    existing_ids = fetch_existing_ids_subset(
        table_fqn=POSTGRES_CUSTOMERS_TABLE,
        id_col="customer_id",
        ids=list(batch_ids),
    )

    missing_ids = batch_ids - existing_ids
    if missing_ids:
        print(
            f"[validate] batch {batch_id}: "
            f"{len(missing_ids)} customer_id(s) in stream but NOT in Postgres ({POSTGRES_CUSTOMERS_TABLE}): "
            f"{sorted(missing_ids)}"
        )
    else:
        print(f"[validate] batch {batch_id}: all {len(batch_ids)} customer_id(s) exist in Postgres")

    valid_ids = batch_ids & existing_ids
    if not valid_ids:
        print(f"[validate] batch {batch_id}: no valid customer_id after lookup; SKIP write")
        return batch_df.limit(0)

    return batch_df.filter(col("customer_id").isin(list(valid_ids)))



def write_customers_to_clickhouse(batch_df: DataFrame, batch_id: int):

    if batch_df.rdd.isEmpty():
        print(f"[dim_customers] batch {batch_id}: empty batch, skipping")
        return

    validated_df = validate_customers_batch(batch_df, batch_id)

    if validated_df.rdd.isEmpty():
        print(f"[dim_customers] batch {batch_id}: no rows after validation, skipping")
        return

    selected_df = (
        validated_df
        .select(
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "region",
            "postal_code",
            "country",
            "phone",
            "fax",
            "op",
            "src_ts",
        )
    )

    rows = selected_df.collect()

    data: List[tuple] = [
        (
            _safe_str(r["customer_id"]),
            _safe_str(r["company_name"]),
            _safe_str(r["contact_name"]),
            _safe_str(r["contact_title"]),
            _safe_str(r["address"]),
            _safe_str(r["city"]),
            _safe_str(r["region"]),
            _safe_str(r["postal_code"]),
            _safe_str(r["country"]),
            _safe_str(r["phone"]),
            _safe_str(r["fax"]),
            _safe_str(r["op"]),
            r["src_ts"],
        )
        for r in rows
    ]

    full_table_name = f"{CLICKHOUSE_DB}.{CLICKHOUSE_CUSTOMERS_TABLE}"

    print(f"[dim_customers] batch {batch_id}: writing {len(data)} row(s) to ClickHouse -> {full_table_name}")

    client = get_clickhouse_client()
    client.execute(
        f"""
        INSERT INTO {full_table_name} (
            customer_id,
            company_name,
            contact_name,
            contact_title,
            address,
            city,
            region,
            postal_code,
            country,
            phone,
            fax,
            op,
            src_ts
        )
        VALUES
        """,
        data,
    )


def main():
    spark = create_spark_session()

    customers_raw_df = read_customers_raw_stream(spark)
    customers_parsed_df = parse_customers_from_debezium(customers_raw_df)

    query = (
        customers_parsed_df
        .writeStream
        .outputMode("append")
        .foreachBatch(write_customers_to_clickhouse)
        .option("checkpointLocation", CHECKPOINT_LOCATION_CUSTOMERS)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("==== Structured Streaming job started ====")
    print("     Kafka topic        :", CUSTOMERS_TOPIC)
    print("     ClickHouse database:", CLICKHOUSE_DB)
    print("     ClickHouse table   :", CLICKHOUSE_CUSTOMERS_TABLE)

    query.awaitTermination()


if __name__ == "__main__":
    main()
