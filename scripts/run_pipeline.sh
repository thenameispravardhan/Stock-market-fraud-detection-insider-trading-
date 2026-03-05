#!/bin/bash
# =============================================================================
# run_pipeline.sh  –  Full fraud detection pipeline orchestrator
#
# Usage:
#   bash scripts/run_pipeline.sh /tmp/orders.csv
#
# Environment variables (override as needed):
#   MYSQL_HOST      default: localhost
#   MYSQL_USER      default: root
#   MYSQL_PASSWORD  (required – no default for security)
#   HADOOP_HOME     (must be set in your environment)
#   HIVE_HOME       (must be set in your environment)
# =============================================================================

set -euo pipefail   # exit on any error, unbound variable, or pipe failure

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

ORDERS_LOCAL="${1:?Usage: $0 /path/to/orders.csv}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# HDFS directory layout for this run
HDFS_BASE="/user/hadoop/fraud"
HDFS_RAW="$HDFS_BASE/raw/$TIMESTAMP"
HDFS_CLEAN="$HDFS_BASE/clean/$TIMESTAMP"
HDFS_PIG="$HDFS_BASE/pig/$TIMESTAMP"
HDFS_HIVE="$HDFS_BASE/hive/$TIMESTAMP"

# MySQL config (read from env or use defaults)
MYSQL_HOST="${MYSQL_HOST:-localhost}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:?Set MYSQL_PASSWORD env variable before running}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }
log "Pipeline run ID: $TIMESTAMP"
log "Orders file   : $ORDERS_LOCAL"

# ── Step 1: Upload orders CSV to HDFS ────────────────────────────────────────
log "=== Step 1: Upload orders to HDFS ==="
hdfs dfs -mkdir -p "$HDFS_RAW"
hdfs dfs -put -f "$ORDERS_LOCAL" "$HDFS_RAW/orders.csv"
log "Orders uploaded to $HDFS_RAW/orders.csv"

# ── Step 2: Fetch NSE stock prices ───────────────────────────────────────────
log "=== Step 2: Fetch NSE prices ==="
python3 "$SCRIPT_DIR/fetch_prices.py" "$ORDERS_LOCAL" "$HDFS_RAW"
# fetch_prices.py writes prices.csv to HDFS_RAW automatically

# ── Step 3: MapReduce – clean both files ─────────────────────────────────────
log "=== Step 3: MapReduce cleaning ==="
# Copy cleaner script to a temp location accessible to Hadoop task nodes
cp "$PROJECT_DIR/mapreduce/cleaner.py" /tmp/cleaner.py
chmod +x /tmp/cleaner.py

hadoop jar \
    "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-"*.jar \
    -D mapreduce.job.name="FraudDetection_Cleaner_$TIMESTAMP" \
    -input  "$HDFS_RAW" \
    -output "$HDFS_CLEAN" \
    -mapper  "python3 cleaner.py" \
    -reducer "org.apache.hadoop.mapred.lib.IdentityReducer" \
    -file    /tmp/cleaner.py

log "Clean data written to $HDFS_CLEAN"

# ── Step 4: Pig – extract large trades ───────────────────────────────────────
log "=== Step 4: Pig large-trade filter ==="
pig -f "$PROJECT_DIR/pig/large_trades.pig" \
    -param input="$HDFS_CLEAN" \
    -param output="$HDFS_PIG"
log "Large trades written to $HDFS_PIG"

# ── Step 5: Hive – aggregation queries ───────────────────────────────────────
log "=== Step 5: Hive aggregations ==="
hive --hiveconf INPUT_PATH="$HDFS_CLEAN" \
     --hiveconf OUTPUT_PATH="$HDFS_HIVE" \
     -f "$PROJECT_DIR/hive/analysis.hql"
log "Hive summaries written to $HDFS_HIVE"

# ── Step 6: Spark – fraud scoring → SQLite ───────────────────────────────────
log "=== Step 6: Spark fraud detection ==="
# DB_PATH is set by the Flask app when it calls this script.
# Fall back to a sensible default if run manually.
: "${DB_PATH:=$(dirname "$SCRIPT_DIR")/data/fraud.db}"

spark-submit \
    --master yarn \
    --deploy-mode client \
    --name "FraudDetection_Spark_$TIMESTAMP" \
    --conf "spark.yarn.appMasterEnv.DB_PATH=$DB_PATH" \
    --conf "spark.executorEnv.DB_PATH=$DB_PATH" \
    "$PROJECT_DIR/spark/fraud_detection.py" \
    "$HDFS_CLEAN"

log "=== Pipeline completed successfully! Run ID: $TIMESTAMP ==="
