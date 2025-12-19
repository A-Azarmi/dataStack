-- postgres/init/02_replication_user.sql

-- 1) ایجاد یوزر مخصوص Debezium برای replication
DROP ROLE IF EXISTS replication_user;

CREATE ROLE replication_user
  WITH REPLICATION LOGIN PASSWORD 'password';

-- 2) سطح دسترسی‌ها
GRANT CONNECT ON DATABASE northwind TO replication_user;
GRANT USAGE ON SCHEMA public TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO replication_user;

-- 3) publication برای Debezium با pgoutput
DROP PUBLICATION IF EXISTS dbz_publication;

CREATE PUBLICATION dbz_publication FOR ALL TABLES;
