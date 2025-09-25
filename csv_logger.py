import csv
import os
import pandas as pd
from datetime import datetime
from loguru import logger

TRADE_LOG_PATH = "./data/trades.csv"

FIELDNAMES = [
    "date", "symbol", "action", "entry", "current", "exit", "pnl_pct"
]

def append_trade_log(new_rows):
    """Add new trades to the log safely."""
    # Fix potential issues with unexpected fields
    sanitized_rows = []
    for row in new_rows:
        sanitized = {key: row.get(key, "") for key in FIELDNAMES}
        if row.get("symbol") and row.get("entry"):
            sanitized_rows.append(sanitized)
        else:
            logger.warning(f"⚠️ Skipping malformed trade row: {row}")

    if not sanitized_rows:
        logger.warning("⚠️ No valid trades to log.")
        return

    # Load existing data if file exists
    existing_rows = []
    if os.path.exists(TRADE_LOG_PATH):
        try:
            existing_rows = pd.read_csv(TRADE_LOG_PATH).to_dict(orient="records")
        except Exception as e:
            logger.warning(f"⚠️ Could not read existing log: {e}")

    # Combine and deduplicate (by symbol+date)
    combined = existing_rows + sanitized_rows
    seen = set()
    final_rows = []
    for row in combined:
        key = (row["date"], row["symbol"])
        if key not in seen:
            final_rows.append(row)
            seen.add(key)

    # Write full cleaned log back
    try:
        with open(TRADE_LOG_PATH, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(final_rows)
        logger.success(f"✅ Logged {len(sanitized_rows)} trade(s).")
    except Exception as e:
        logger.error(f"❌ Failed to write trades.csv: {e}")
