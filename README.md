# Stock Market Fraud Detection System

A big-data-inspired pipeline for detecting suspicious stock trades, with a Flask web dashboard for results visualization. Designed to run fully on a local Windows machine without requiring Hadoop, Spark, or any cloud infrastructure.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [How the Pipeline Works](#how-the-pipeline-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Application](#running-the-application)
- [Using the Dashboard](#using-the-dashboard)
- [Sample Data Format](#sample-data-format)
- [Database](#database)

---

## Overview

This system analyzes stock trade orders to identify potentially fraudulent activity such as insider trading or abnormal volume spikes. It mimics a real big-data pipeline (HDFS, MapReduce, Pig, Hive, Spark) using pure Python and pandas — making it fully runnable on Windows without any Hadoop setup.

Detected suspicious trades are stored in a local SQLite database and presented through a Flask web dashboard.

---

## Project Structure

```
fraud_detection/
├── flask_app/
│   ├── app.py                  # Flask web application
│   ├── templates/              # HTML templates
│   └── static/                 # CSS / JS assets
├── scripts/
│   ├── local_pipeline.py       # Local 6-step pipeline (Windows-friendly)
│   ├── fetch_prices.py         # NSE price fetcher
│   └── run_pipeline.sh         # Bash pipeline script (for Linux/Cluster)
├── spark/
│   └── fraud_detection.py      # PySpark fraud scoring job (for cluster use)
├── hive/                       # Hive query scripts
├── mapreduce/                  # MapReduce scripts
├── pig/                        # Pig Latin scripts
├── data/
│   ├── orders.csv              # Sample input trade orders
│   ├── fraud.db                # SQLite database (auto-created)
│   ├── raw/                    # Raw uploaded data (pipeline output)
│   ├── clean/                  # Cleaned data (pipeline output)
│   ├── pig/                    # Pig-stage output
│   ├── hive/                   # Hive-stage output
│   └── suspicious_trades.csv   # Latest detected suspicious trades
└── requirements.txt
```

---

## How the Pipeline Works

When a CSV file of trade orders is uploaded, the system runs a 6-step local pipeline:

| Step | Name | Description |
|------|------|-------------|
| 1 | HDFS Upload | Copies the uploaded CSV to `data/raw/` |
| 2 | Fetch Prices | Fetches NSE closing prices via `nsepy`; falls back to synthetic data if unavailable |
| 3 | MapReduce | Validates and cleans orders — drops nulls, negative quantities, and bad prices |
| 4 | Pig | Flags trades where quantity exceeds `mean + 3 * stddev` for that stock |
| 5 | Hive | Generates per-trader and per-stock aggregation summaries as CSV files |
| 6 | Spark | Scores each trade (0–100) based on volume z-score and price spike; writes results to SQLite |

**Scoring logic:**
- `size_score` (0–60): based on z-score of trade quantity vs. per-stock average
- `price_score` (0–40): awarded when stock price increased by more than 2% after the trade
- Trades with a combined `suspicion_score > 50` are flagged as suspicious

---

## Prerequisites

- Python 3.9 or higher
- pip

No Hadoop, Spark, or MySQL installation required.

---

## Installation

1. Clone or download the repository:
   ```
   git clone <repo-url>
   cd fraud_detection
   ```

2. Install Python dependencies:
   ```
   pip install -r requirements.txt
   ```

The `sqlite3` module is part of Python's standard library and requires no additional installation.

---

## Running the Application

Start the Flask development server:

```bash
cd flask_app
python app.py
```

The application will be available at: [http://localhost:5000](http://localhost:5000)

The SQLite database (`data/fraud.db`) is automatically created on first run.

---

## Using the Dashboard

1. **Upload Orders**: Click "Upload CSV" and select a trade orders file.
2. **View Results**: Suspicious trades are listed in the dashboard table, sorted by suspicion score (highest first).
3. **Download Sample**: Click "Download Sample CSV" to get a sample input file to test the pipeline.
4. **Clear Results**: Use the "Clear All" button to wipe the current results from the database.

The dashboard also displays summary statistics:
- **Total** suspicious trades detected
- **Critical** trades (score >= 90)
- **Moderate** trades (score 70–89)

---

## Sample Data Format

The input CSV must have the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `trader_id` | string | Unique trader identifier (e.g., `T201`) |
| `stock` | string | NSE stock symbol (e.g., `TCS`, `INFY`) |
| `quantity` | integer | Number of shares traded |
| `price` | float | Trade price per share |
| `timestamp` | string | Time of trade (e.g., `10:10`) |

**Example:**
```csv
trader_id,stock,quantity,price,timestamp
T201,TCS,10000,3500,10:10
T202,INFY,200,1470,10:15
T203,RELIANCE,15000,2500,10:20
```

A ready-to-use sample file is available from the dashboard's "Download Sample CSV" button.

---

## Database

Results are stored in a local SQLite database at `data/fraud.db`.

**Table: `suspicious_trades`**

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-incremented primary key |
| `trader_id` | TEXT | Trader identifier |
| `stock` | TEXT | Stock symbol |
| `quantity` | INTEGER | Number of shares in the trade |
| `reason` | TEXT | Human-readable reason for flagging |
| `suspicion_score` | INTEGER | Score from 0 to 100 |
| `detected_at` | TIMESTAMP | When the record was written |

The database is fully managed by the application — no manual setup is needed.
