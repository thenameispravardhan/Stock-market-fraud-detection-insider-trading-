#!/usr/bin/env python3
"""
cleaner.py  –  Hadoop Streaming Mapper + Reducer (identity reducer is used).

Reads CSV lines from stdin, validates fields, and emits cleaned records
prefixed with ORDER or PRICE so the downstream steps can separate them.

Run via Hadoop Streaming:
    hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -input  /user/hadoop/fraud/raw/<ts> \
        -output /user/hadoop/fraud/clean/<ts> \
        -mapper "python3 cleaner.py" \
        -reducer org.apache.hadoop.mapred.lib.IdentityReducer \
        -file cleaner.py
"""

import sys


ORDERS_HEADER = {'trader_id'}
PRICES_HEADER = {'datetime'}


def clean_line(line: str) -> None:
    line = line.strip()
    if not line:
        return

    parts = line.split(',')

    # ── Skip any header row ──────────────────────────────────────────────────
    if parts[0].strip().lower() in ORDERS_HEADER or \
       parts[0].strip().lower() in PRICES_HEADER:
        return

    # ── Orders: trader_id, stock, quantity, price, timestamp ────────────────
    if len(parts) == 5:
        trader_id, stock, quantity, price, timestamp = (p.strip() for p in parts)
        try:
            qty   = int(quantity)
            px    = float(price)
            if qty <= 0 or px <= 0 or not stock or not trader_id:
                return
            # Emit ORDER record
            print(f"ORDER\t{trader_id},{stock},{qty},{px:.4f},{timestamp}")
        except ValueError:
            pass  # silently drop malformed rows

    # ── Prices: datetime, stock, price ──────────────────────────────────────
    elif len(parts) == 3:
        datetime_str, stock, price = (p.strip() for p in parts)
        try:
            px = float(price)
            if px <= 0 or not stock:
                return
            print(f"PRICE\t{datetime_str},{stock},{px:.4f}")
        except ValueError:
            pass


def main():
    for raw_line in sys.stdin:
        clean_line(raw_line)


if __name__ == '__main__':
    main()
