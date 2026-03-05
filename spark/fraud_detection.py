"""
fraud_detection.py  –  PySpark job for insider trading detection.

Usage (called by run_pipeline.sh):
    spark-submit spark/fraud_detection.py <hdfs_clean_dir>

DB_PATH env variable must point to the SQLite database file.

The job:
  1. Reads cleaned ORDER and PRICE records from hdfs_clean_dir.
  2. Joins orders with the nearest post-trade price.
  3. Computes a suspicion_score (0-100) based on:
       - Normalised trade-size deviation (z-score) → size_score
       - Post-trade price change percentage        → price_score
  4. Flags trades with score > 70 as suspicious.
  5. Writes results to SQLite suspicious_trades table.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


# ── Schema helpers ────────────────────────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("record_type", StringType(), True),
    StructField("trader_id",   StringType(), True),
    StructField("stock",       StringType(), True),
    StructField("quantity",    IntegerType(), True),
    StructField("price",       FloatType(),  True),
    StructField("trade_time",  StringType(), True),
])

PRICE_SCHEMA = StructType([
    StructField("record_type",    StringType(), True),
    StructField("price_datetime", StringType(), True),
    StructField("stock",          StringType(), True),
    StructField("price",          FloatType(),  True),
])


# ── SQLite writer ─────────────────────────────────────────────────────────────

def write_to_sqlite(rows, db_path):
    """Collect Spark rows on the driver and bulk-insert into SQLite."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS suspicious_trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            trader_id       TEXT,
            stock           TEXT,
            quantity        INTEGER,
            reason          TEXT,
            suspicion_score INTEGER,
            detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Clear previous run
    conn.execute("DELETE FROM suspicious_trades")

    conn.executemany(
        "INSERT INTO suspicious_trades (trader_id, stock, quantity, reason, suspicion_score) "
        "VALUES (?, ?, ?, ?, ?)",
        [(row.trader_id, row.stock, int(row.quantity), row.reason, int(row.suspicion_score))
         for row in rows]
    )
    conn.commit()
    conn.close()
    print(f"[SQLite] Written {len(rows)} suspicious trade(s) to {db_path}.")


# ── Main detection logic ──────────────────────────────────────────────────────

def detect_fraud(clean_hdfs_dir: str, mysql_host: str, mysql_user: str, mysql_password: str):
    spark = SparkSession.builder \
        .appName("StockFraudDetection") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Load raw MapReduce output ─────────────────────────────────────────
    raw = spark.read.text(clean_hdfs_dir)

    # Split tab-separated "TYPE\tfield1,field2,..." into columns
    split_col = F.split(raw["value"], "\t", 2)
    raw = raw.withColumn("record_type", split_col[0]) \
             .withColumn("payload",     split_col[1])

    # ── 2. Parse ORDER records ───────────────────────────────────────────────
    order_fields = F.split(F.col("payload"), ",", 5)
    orders = raw.filter(F.col("record_type") == "ORDER") \
        .select(
            order_fields[0].alias("trader_id"),
            order_fields[1].alias("stock"),
            order_fields[2].cast("int").alias("quantity"),
            order_fields[3].cast("float").alias("trade_price"),
            order_fields[4].alias("trade_time_str"),
        ) \
        .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))

    # Convert trade_time (HH:mm) to seconds-since-midnight for arithmetic
    orders = orders.withColumn(
        "trade_seconds",
        F.unix_timestamp(F.col("trade_time_str"), "HH:mm")
    )

    # ── 3. Parse PRICE records ───────────────────────────────────────────────
    price_fields = F.split(F.col("payload"), ",", 3)
    prices = raw.filter(F.col("record_type") == "PRICE") \
        .select(
            price_fields[0].alias("price_date"),
            price_fields[1].alias("stock"),
            price_fields[2].cast("float").alias("close_price"),
        ) \
        .filter(F.col("close_price").isNotNull() & (F.col("close_price") > 0))

    # We treat the uploaded orders' intraday times relative to NSE daily prices.
    # Strategy: for each stock, get average close price and next-day close price.
    window_stock = Window.partitionBy("stock").orderBy("price_date")
    prices = prices.withColumn("next_close", F.lead("close_price", 1).over(window_stock))

    # ── 4. Per-stock statistics (for anomaly threshold) ──────────────────────
    stock_stats = orders.groupBy("stock").agg(
        F.avg("quantity").alias("avg_qty"),
        F.stddev("quantity").alias("stddev_qty"),
        F.count("*").alias("trade_count"),
    )

    # ── 5. Join orders with stock stats ─────────────────────────────────────
    orders_with_stats = orders.join(stock_stats, on="stock", how="left")

    # ── 6. Join with latest available price per stock ────────────────────────
    # Take the most recent price date for each stock
    latest_price = prices.orderBy(F.col("price_date").desc()) \
                         .dropDuplicates(["stock"])

    joined = orders_with_stats.join(
        latest_price.select(
            F.col("stock").alias("p_stock"),
            F.col("close_price").alias("current_close"),
            F.col("next_close"),
        ),
        orders_with_stats.stock == F.col("p_stock"),
        "left",
    )

    # ── 7. Price change percentage (next day close vs. current) ─────────────
    joined = joined.withColumn(
        "price_change_pct",
        F.when(
            F.col("current_close").isNotNull() & F.col("next_close").isNotNull() &
            (F.col("current_close") > 0),
            (F.col("next_close") - F.col("current_close")) / F.col("current_close") * 100
        ).otherwise(F.lit(0.0))
    )

    # ── 8. Compute suspicion score ───────────────────────────────────────────
    # size_score: z-score capped at 0–60
    joined = joined.withColumn(
        "size_score",
        F.when(
            F.col("stddev_qty").isNotNull() & (F.col("stddev_qty") > 0),
            F.least(
                F.lit(60.0),
                F.greatest(
                    F.lit(0.0),
                    (F.col("quantity") - F.col("avg_qty")) / F.col("stddev_qty") * 15
                )
            )
        ).otherwise(F.lit(0.0))
    )

    # price_score: up to 40 points for post-trade price spike > 2%
    joined = joined.withColumn(
        "price_score",
        F.when(F.col("price_change_pct") > 2.0,
               F.least(F.lit(40.0), F.col("price_change_pct") * 4.0)
               ).otherwise(F.lit(0.0))
    )

    joined = joined.withColumn(
        "suspicion_score",
        F.least(F.lit(100.0), F.col("size_score") + F.col("price_score")).cast("int")
    )

    # ── 9. Flag suspicious trades ────────────────────────────────────────────
    suspicious = joined.filter(F.col("suspicion_score") > 70).withColumn(
        "reason",
        F.when(
            (F.col("quantity") > F.col("avg_qty") + 3 * F.col("stddev_qty")) &
            (F.col("price_change_pct") > 5),
            F.lit("Large trade immediately before a price spike (probable insider trading)")
        ).when(
            F.col("quantity") > F.col("avg_qty") + 3 * F.col("stddev_qty"),
            F.lit("Abnormally large trade volume relative to stock average")
        ).when(
            F.col("price_change_pct") > 5,
            F.lit("Trade followed by significant price increase (>5%)")
        ).otherwise(
            F.lit("Combined volume and price anomaly detected")
        )
    ).select(
        "trader_id", "stock", "quantity", "reason", "suspicion_score"
    )

    suspicious.show(truncate=False)

    # ── 10. Write results to SQLite ───────────────────────────────────────────
    rows = suspicious.collect()
    db_path = db_path_arg
    if rows:
        write_to_sqlite(rows, db_path)
    else:
        print("[INFO] No suspicious trades found this run.")

    spark.stop()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import os
    if len(sys.argv) < 2:
        print(
            "Usage: spark-submit fraud_detection.py <hdfs_clean_dir>",
            file=sys.stderr,
        )
        sys.exit(1)

    db_path_env = os.environ.get('DB_PATH', '/tmp/fraud.db')
    detect_fraud(
        clean_hdfs_dir = sys.argv[1],
        db_path_arg    = db_path_env,
    )
