-- ============================================================
-- analysis.hql  –  Hive Analysis for Fraud Detection Pipeline
-- ============================================================
-- Run:  hive --hiveconf INPUT_PATH=<hdfs_clean_dir> \
--             --hiveconf OUTPUT_PATH=<hdfs_hive_dir> \
--             -f analysis.hql

-- Drop and recreate the external table so it always points to the
-- latest HDFS clean directory passed via the pipeline.
DROP TABLE IF EXISTS orders;

CREATE EXTERNAL TABLE orders (
    record_type STRING,       -- ORDER prefix from MapReduce output
    trader_id   STRING,
    stock       STRING,
    quantity    INT,
    price       FLOAT,
    trade_time  STRING
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'   -- tab between prefix and CSV payload
LOCATION '${hiveconf:INPUT_PATH}';

-- ── 1. Trader summary: total trades and volume per trader ──
INSERT OVERWRITE DIRECTORY '${hiveconf:OUTPUT_PATH}/trader_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    trader_id,
    COUNT(*)        AS trade_count,
    SUM(quantity)   AS total_quantity,
    AVG(price)      AS avg_price,
    MAX(price)      AS max_price
FROM orders
WHERE record_type = 'ORDER'
GROUP BY trader_id
ORDER BY total_quantity DESC;

-- ── 2. Stock summary: average trade size per stock ──
INSERT OVERWRITE DIRECTORY '${hiveconf:OUTPUT_PATH}/stock_summary'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    stock,
    COUNT(*)         AS trade_count,
    AVG(quantity)    AS avg_quantity,
    STDDEV(quantity) AS stddev_quantity,
    AVG(price)       AS avg_price
FROM orders
WHERE record_type = 'ORDER'
GROUP BY stock
ORDER BY avg_quantity DESC;

-- ── 3. High-volume traders (total quantity > 20,000 across all stocks) ──
INSERT OVERWRITE DIRECTORY '${hiveconf:OUTPUT_PATH}/high_volume_traders'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    trader_id,
    SUM(quantity) AS total_volume
FROM orders
WHERE record_type = 'ORDER'
GROUP BY trader_id
HAVING SUM(quantity) > 20000
ORDER BY total_volume DESC;
