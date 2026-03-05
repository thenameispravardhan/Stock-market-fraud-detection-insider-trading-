#!/usr/bin/env python3
"""
fetch_prices.py  –  Read uploaded orders.csv, extract unique NSE stock symbols
and date range, fetch historical OHLC data via nsepy, and upload the price CSV
to HDFS.

Usage:
    python fetch_prices.py <orders_csv_path> <hdfs_output_dir>
"""

import sys
import os
import pandas as pd
from datetime import timedelta

# nsepy is the standard open-source library for NSE historical data (no API key needed)
try:
    from nsepy import get_history
except ImportError:
    print("[ERROR] nsepy not installed. Run: pip install nsepy", file=sys.stderr)
    sys.exit(1)


def fetch_prices(orders_path: str, hdfs_output_dir: str) -> None:
    # ── 1. Read orders ──────────────────────────────────────────────────────
    orders = pd.read_csv(orders_path)

    # Expect columns: trader_id, stock, quantity, price, timestamp
    required = {'trader_id', 'stock', 'quantity', 'price', 'timestamp'}
    if not required.issubset(orders.columns):
        print(f"[ERROR] orders.csv must contain columns: {required}", file=sys.stderr)
        sys.exit(1)

    # Parse timestamps (time-only like "10:15" needs a base date; use today's date)
    base_date = pd.Timestamp.today().normalize()
    orders['datetime'] = pd.to_datetime(
        base_date.strftime('%Y-%m-%d') + ' ' + orders['timestamp'].astype(str),
        errors='coerce'
    )

    stocks = orders['stock'].dropna().unique().tolist()
    start_date = (base_date - timedelta(days=30)).date()   # 30-day lookback
    end_date   = base_date.date()

    print(f"[INFO] Fetching prices for {stocks} from {start_date} to {end_date}")

    # ── 2. Fetch from NSE ───────────────────────────────────────────────────
    all_prices = []
    for symbol in stocks:
        try:
            data = get_history(symbol=symbol, start=start_date, end=end_date)
            if data is None or data.empty:
                print(f"[WARN] No data returned for {symbol}", file=sys.stderr)
                continue
            data = data.reset_index()[['Date', 'Close']]
            data['stock'] = symbol
            data.rename(columns={'Date': 'datetime', 'Close': 'price'}, inplace=True)
            all_prices.append(data)
            print(f"[OK]   {symbol}: {len(data)} rows")
        except Exception as exc:
            print(f"[WARN] Could not fetch {symbol}: {exc}", file=sys.stderr)

    if not all_prices:
        print("[ERROR] No price data fetched for any symbol.", file=sys.stderr)
        sys.exit(1)

    # ── 3. Save locally then push to HDFS ───────────────────────────────────
    prices_df = pd.concat(all_prices, ignore_index=True)
    local_csv  = '/tmp/prices.csv'
    prices_df.to_csv(local_csv, index=False)
    print(f"[INFO] Saved {len(prices_df)} price rows to {local_csv}")

    hdfs_target = f"{hdfs_output_dir}/prices.csv"
    ret = os.system(f'hdfs dfs -put -f "{local_csv}" "{hdfs_target}"')
    if ret != 0:
        print(f"[ERROR] hdfs dfs -put failed (exit {ret})", file=sys.stderr)
        sys.exit(1)
    print(f"[INFO] Prices uploaded to HDFS: {hdfs_target}")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python fetch_prices.py <orders_csv_path> <hdfs_output_dir>",
              file=sys.stderr)
        sys.exit(1)
    fetch_prices(sys.argv[1], sys.argv[2])
