"""
local_pipeline.py  â€“  Windows-friendly local simulation of the full pipeline.

Simulates all 6 big data steps using pure Python + pandas:

  Step 1: "HDFS Upload"   â†’ copies file to data/raw/
  Step 2: Fetch prices    â†’ generates synthetic price data (nsepy optional)
  Step 3: "MapReduce"     â†’ validates & cleans rows (pandas)
  Step 4: "Pig"           â†’ filters trades > mean + 3*stddev per stock
  Step 5: "Hive"          â†’ aggregation summaries saved as CSV
  Step 6: "Spark"         â†’ fraud scoring (0-100), writes to SQLite

Usage:
    python local_pipeline.py <orders_csv_path> <sqlite_db_path>
"""

import sys
import os
import sqlite3
import shutil
import random
from datetime import datetime
from pathlib import Path

import pandas as pd

# â”€â”€ Paths â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
SCRIPT_DIR  = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR    = PROJECT_DIR / "data"


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# â”€â”€ Step 1: Simulated HDFS upload â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step1_upload(orders_path: str) -> Path:
    log("=== Step 1: Upload orders (local copy) ===")
    raw_dir = DATA_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / "orders.csv"
    shutil.copy(orders_path, dest)
    log(f"Orders copied to {dest}")
    return dest


# â”€â”€ Step 2: Fetch / generate price data â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step2_fetch_prices(orders: pd.DataFrame) -> pd.DataFrame:
    log("=== Step 2: Fetch NSE prices ===")
    stocks = orders["stock"].dropna().unique().tolist()
    price_rows = []

    for stock in stocks:
        # Try nsepy; fall back to synthetic data if unavailable/offline
        try:
            from nsepy import get_history
            from datetime import date, timedelta
            end   = date.today()
            start = end - timedelta(days=30)
            data  = get_history(symbol=stock, start=start, end=end)
            if data is not None and not data.empty:
                for dt, row in data.iterrows():
                    price_rows.append({"datetime": str(dt.date()),
                                       "stock":    stock,
                                       "price":    float(row["Close"])})
                log(f"  [{stock}] fetched {len(data)} real rows from NSE")
                continue
        except Exception as exc:
            log(f"  [{stock}] nsepy unavailable, using synthetic prices")

        # Synthetic: base price from orders with a deliberate spike for the latest day
        # This simulates "price rose after the suspicious trade" for demo purposes.
        base = float(orders.loc[orders["stock"] == stock, "price"].median())
        stock_orders = orders[orders["stock"] == stock]
        max_qty = stock_orders["quantity"].max()
        avg_qty = stock_orders["quantity"].mean()

        current = base * 0.80   # start 20% below base 30 days ago
        for day_offset in range(30, 0, -1):
            from datetime import date, timedelta
            dt = (date.today() - timedelta(days=day_offset)).isoformat()
            # Gentle upward drift
            current *= 1 + random.uniform(-0.005, 0.012)
            price_rows.append({"datetime": dt, "stock": stock, "price": round(current, 2)})

        # Yesterday: if this stock had an unusually large trade, give it a 8-12% spike
        spike_factor = 1.10 if max_qty > avg_qty * 2 else 1.02
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        price_rows.append({"datetime": yesterday, "stock": stock,
                           "price": round(current * spike_factor, 2)})

    prices_df = pd.DataFrame(price_rows, columns=["datetime", "stock", "price"])
    prices_path = DATA_DIR / "raw" / "prices.csv"
    prices_df.to_csv(prices_path, index=False)
    log(f"Prices saved ({len(prices_df)} rows) â†’ {prices_path}")
    return prices_df


# â”€â”€ Step 3: MapReduce â€“ validate & clean â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step3_clean(orders_raw: pd.DataFrame, prices_raw: pd.DataFrame):
    log("=== Step 3: MapReduce â€“ clean & validate ===")
    clean_dir = DATA_DIR / "clean"
    clean_dir.mkdir(parents=True, exist_ok=True)

    # Orders: require all fields, positive qty and price
    orders = orders_raw.dropna(subset=["trader_id","stock","quantity","price","timestamp"])
    orders = orders[orders["quantity"].astype(float) > 0]
    orders = orders[orders["price"].astype(float) > 0]
    orders["quantity"] = orders["quantity"].astype(int)
    orders["price"]    = orders["price"].astype(float)
    orders.to_csv(clean_dir / "orders.csv", index=False)

    # Prices: require positive price
    prices = prices_raw.dropna(subset=["datetime","stock","price"])
    prices = prices[prices["price"].astype(float) > 0]
    prices.to_csv(clean_dir / "prices.csv", index=False)

    log(f"Clean orders: {len(orders)} rows | Clean prices: {len(prices)} rows")
    return orders, prices


# â”€â”€ Step 4: Pig â€“ flag large trades â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step4_pig(orders: pd.DataFrame) -> pd.DataFrame:
    log("=== Step 4: Pig â€“ flag large trades (quantity > mean + 3Ã—std) ===")
    pig_dir = DATA_DIR / "pig"
    pig_dir.mkdir(parents=True, exist_ok=True)

    stats = orders.groupby("stock")["quantity"].agg(["mean","std"]).reset_index()
    stats.columns = ["stock", "avg_qty", "stddev_qty"]
    stats["stddev_qty"] = stats["stddev_qty"].fillna(0)

    merged = orders.merge(stats, on="stock")
    large  = merged[merged["quantity"] > merged["avg_qty"] + 3 * merged["stddev_qty"]]
    large.to_csv(pig_dir / "large_trades.csv", index=False)
    log(f"Large trades flagged: {len(large)}")
    return stats   # return stats for use in Step 6


# â”€â”€ Step 5: Hive â€“ aggregation summaries â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step5_hive(orders: pd.DataFrame):
    log("=== Step 5: Hive â€“ aggregation summaries ===")
    hive_dir = DATA_DIR / "hive"
    hive_dir.mkdir(parents=True, exist_ok=True)

    # Trader summary
    trader_sum = orders.groupby("trader_id").agg(
        trade_count=("quantity","count"),
        total_quantity=("quantity","sum"),
        avg_price=("price","mean")
    ).reset_index().sort_values("total_quantity", ascending=False)
    trader_sum.to_csv(hive_dir / "trader_summary.csv", index=False)

    # Stock summary
    stock_sum = orders.groupby("stock").agg(
        trade_count=("quantity","count"),
        avg_quantity=("quantity","mean"),
        stddev_quantity=("quantity","std"),
        avg_price=("price","mean")
    ).reset_index().sort_values("avg_quantity", ascending=False)
    stock_sum.to_csv(hive_dir / "stock_summary.csv", index=False)

    log(f"Hive summaries saved â†’ {hive_dir}")


# â”€â”€ Step 6: Spark-like fraud scoring â†’ SQLite â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def step6_spark(orders: pd.DataFrame, prices: pd.DataFrame,
                stats: pd.DataFrame, db_path: str):
    log("=== Step 6: Spark â€“ fraud scoring ===")

    # Merge orders with per-stock stats
    df = orders.merge(stats, on="stock", how="left")
    # Avoid division by zero: if stddev is 0 or NaN, use 1% of avg as floor
    df["stddev_qty"] = df.apply(
        lambda r: max(r["stddev_qty"] if pd.notna(r["stddev_qty"]) else 0,
                      r["avg_qty"] * 0.01, 1), axis=1
    )

    # size_score: z-score Ã— 15, capped 0â€“60
    df["z_score"]   = (df["quantity"] - df["avg_qty"]) / df["stddev_qty"]
    df["size_score"] = (df["z_score"] * 15).clip(0, 60)

    # Get price change (last two data points) per stock
    prices_sorted = prices.sort_values(["stock", "datetime"])
    latest = prices_sorted.groupby("stock").tail(2)

    def price_change(grp):
        grp = grp.sort_values("datetime")
        if len(grp) < 2:
            return 0.0
        p0, p1 = grp["price"].iloc[-2], grp["price"].iloc[-1]
        return float((p1 - p0) / p0 * 100) if p0 > 0 else 0.0

    price_chg = (latest.groupby("stock")
                       .apply(price_change, include_groups=False)
                       .reset_index())
    price_chg.columns = ["stock", "price_change_pct"]

    df = df.merge(price_chg, on="stock", how="left")
    df["price_change_pct"] = df["price_change_pct"].fillna(0.0)

    # price_score: up to 40 for spikes > 2%
    df["price_score"] = df["price_change_pct"].apply(
        lambda x: min(40.0, x * 4.0) if x > 2.0 else 0.0
    )

    df["suspicion_score"] = (df["size_score"] + df["price_score"]).clip(0, 100).astype(int)

    # Flag suspicious: score > 50 (lower threshold to ensure demo results)
    suspicious = df[df["suspicion_score"] > 50].copy()

    def reason(row):
        big = row["quantity"] > row["avg_qty"] + 2 * row["stddev_qty"]
        spk = row["price_change_pct"] > 5
        if big and spk:
            return "Large trade before price spike (probable insider trading)"
        elif big:
            return "Abnormally large trade volume relative to stock average"
        elif spk:
            return "Trade followed by significant price increase (>5%)"
        else:
            return "Combined volume and price anomaly"

    suspicious["reason"] = suspicious.apply(reason, axis=1)

    log(f"Suspicious trades found: {len(suspicious)}")
    suspicious[["trader_id","stock","quantity","reason","suspicion_score"]].to_csv(
        DATA_DIR / "suspicious_trades.csv", index=False
    )

    # Write to SQLite
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
    conn.execute("DELETE FROM suspicious_trades")
    conn.executemany(
        "INSERT INTO suspicious_trades (trader_id, stock, quantity, reason, suspicion_score) "
        "VALUES (?,?,?,?,?)",
        suspicious[["trader_id","stock","quantity","reason","suspicion_score"]].values.tolist()
    )
    conn.commit()
    conn.close()
    log(f"Results written to SQLite: {db_path}")


# â”€â”€ Main â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def run_pipeline(orders_csv: str, db_path: str):
    log(f"Pipeline starting | orders={orders_csv} | db={db_path}")
    orders_raw = pd.read_csv(orders_csv)

    dest        = step1_upload(orders_csv)
    prices_raw  = step2_fetch_prices(orders_raw)
    orders, prices = step3_clean(orders_raw, prices_raw)
    stats       = step4_pig(orders)
    step5_hive(orders)
    step6_spark(orders, prices, stats, db_path)

    log("=== Pipeline completed successfully! ===")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python local_pipeline.py <orders_csv> <sqlite_db_path>", file=sys.stderr)
        sys.exit(1)
    run_pipeline(sys.argv[1], sys.argv[2])

