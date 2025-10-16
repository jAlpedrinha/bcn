-- Example Iceberg Table Operations

-- 1. Create a database
CREATE DATABASE IF NOT EXISTS iceberg.demo;

-- 2. Create an Iceberg table
CREATE TABLE iceberg.demo.users (
  id BIGINT,
  name STRING,
  email STRING,
  age INT,
  created_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(created_at));

-- 3. Insert data
INSERT INTO iceberg.demo.users VALUES 
  (1, 'Alice', 'alice@example.com', 30, current_timestamp()),
  (2, 'Bob', 'bob@example.com', 25, current_timestamp()),
  (3, 'Charlie', 'charlie@example.com', 35, current_timestamp());

-- 4. Query the table
SELECT * FROM iceberg.demo.users;

-- 5. Show table metadata
DESCRIBE EXTENDED iceberg.demo.users;

-- 6. Show table history (Iceberg time travel feature)
SELECT * FROM iceberg.demo.users.history;

-- 7. Show table snapshots
SELECT * FROM iceberg.demo.users.snapshots;

-- 8. Update data (using merge)
MERGE INTO iceberg.demo.users t
USING (SELECT 2 as id, 26 as age) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.age = s.age;

-- 9. Time travel - query previous snapshot
-- SELECT * FROM iceberg.demo.users VERSION AS OF <snapshot_id>;

-- 10. Show all tables
SHOW TABLES IN iceberg.demo;
