-- ============================================================
-- large_trades.pig
-- Identifies unusually large trades: quantity > mean + 3*stddev
-- per stock. Output is stored in HDFS for Spark consumption.
-- ============================================================

-- 1. Load cleaned order records (tab-prefixed line format from MapReduce)
--    Each record looks like: ORDER\ttrader_id,stock,quantity,price,timestamp
--    We strip the prefix using a simple REGEX tokenizer.

raw = LOAD '$input' USING TextLoader() AS (raw_line:chararray);

-- Keep only ORDER rows and split on the tab
order_rows = FILTER raw BY raw_line MATCHES '^ORDER.*';
parsed = FOREACH order_rows GENERATE
    TRIM(REGEX_EXTRACT(raw_line, '^ORDER\\t(.*)$', 1)) AS csv_part;

-- Split CSV part into individual fields
orders = FOREACH parsed GENERATE
    STRSPLIT(csv_part, ',', 5) AS fields;

orders_flat = FOREACH orders GENERATE
    (chararray) fields.$0 AS trader_id,
    (chararray) fields.$1 AS stock,
    (int)       fields.$2 AS quantity,
    (float)     fields.$3 AS price,
    (chararray) fields.$4 AS timestamp;

-- 2. Per-stock statistics: average and standard deviation of quantity
grouped   = GROUP orders_flat BY stock;
stock_stats = FOREACH grouped GENERATE
    group     AS stock,
    AVG(orders_flat.quantity)    AS avg_qty,
    -- Pig does not have a built-in STDEV; compute it manually via variance
    SQRT(
        AVG(
            (double)(orders_flat.quantity * orders_flat.quantity)
        ) - AVG((double)orders_flat.quantity) * AVG((double)orders_flat.quantity)
    ) AS stddev_qty;

-- 3. Join trades with statistics to find outliers
joined = JOIN orders_flat BY stock, stock_stats BY stock;

large_trades = FILTER joined BY
    (double) orders_flat::quantity > (stock_stats::avg_qty + 3.0 * stock_stats::stddev_qty);

-- 4. Project only the columns needed downstream
result = FOREACH large_trades GENERATE
    orders_flat::trader_id AS trader_id,
    orders_flat::stock     AS stock,
    orders_flat::quantity  AS quantity,
    orders_flat::price     AS price,
    orders_flat::timestamp AS timestamp,
    stock_stats::avg_qty   AS avg_qty,
    stock_stats::stddev_qty AS stddev_qty;

-- 5. Store to HDFS
STORE result INTO '$output' USING PigStorage(',');
