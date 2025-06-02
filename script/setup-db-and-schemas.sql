-- First: Create a database in duckdb
-- duckdb data_warehouse.duckdb

-- Second: Create schemas (bronze, silver, gold)
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- List schemas to ensure if they are created
SELECT schema_name FROM information_schema.schemata;
