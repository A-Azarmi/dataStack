CREATE DATABASE IF NOT EXISTS northwind_dw;

CREATE TABLE IF NOT EXISTS northwind_dw.order_wide
(
    order_id        Int32,
    order_date      Date,

    
    customer_id     String,
    customer_name   String,

    
    employee_id         Int32,
    employee_name       String,
    employee_photo_url  String,


    product_id      Int32,
    product_name    String,
    category_name   String,
    supplier_name   String,


    unit_price      Decimal(10, 2),
    quantity        Int16,
    discount        Decimal(10, 2),
    total_amount    Decimal(15, 2),

    _version        UInt64,
    _updated_at     DateTime,
    _source_ts_ms   Int64
)
ENGINE = ReplacingMergeTree(_version)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_id, product_id);
