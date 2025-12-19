CREATE DATABASE IF NOT EXISTS northwind_dw;

CREATE TABLE IF NOT EXISTS northwind_dw.dim_customers
(
  customer_id   String,
  company_name  String,
  contact_name  Nullable(String),
  contact_title Nullable(String),
  address       Nullable(String),
  city          Nullable(String),
  region        Nullable(String),
  postal_code   Nullable(String),
  country       Nullable(String),
  phone         Nullable(String),
  fax           Nullable(String),

  _version      UInt64,
  _updated_at   DateTime,
  _source_ts_ms Int64
)
ENGINE = ReplacingMergeTree(_version)
ORDER BY (customer_id);
