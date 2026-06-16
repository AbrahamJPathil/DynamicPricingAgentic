"""
load_products_common.py

Loads / refreshes the `products_common` table in the SQLite database from a
CSV file. Safe to re-run: rows are matched on sku_id, so re-running with an
updated CSV updates existing products in place and inserts any new ones,
instead of duplicating rows or wiping the table.

This is the first table in what will become a multi-table database, so the
table is named `products_common` (not just `products`) to leave room for
other tables (e.g. price history, sales, suppliers) to be added later
without naming collisions.

Usage:
    python load_products_common.py
    python load_products_common.py --csv data/products_common.csv --db store.db
"""

import argparse
import csv
import sqlite3
from pathlib import Path

DEFAULT_CSV = Path(__file__).parent / "data" / "products_common.csv"
DEFAULT_DB = Path(__file__).parent / "store.db"

# SQLite has no native boolean or datetime type, so:
#   - is_perishable is stored as INTEGER 0/1 (with a CHECK constraint)
#   - timestamps (batch_received_at, expiry_datetime) are stored as TEXT in
#     ISO-8601 form, exactly as they appear in the source data
SCHEMA = """
CREATE TABLE IF NOT EXISTS products_common (
    sku_id               TEXT    PRIMARY KEY,
    product_name         TEXT    NOT NULL,
    category             TEXT    NOT NULL,
    unit                 TEXT    NOT NULL,
    our_price            REAL    NOT NULL,
    cost_price            REAL    NOT NULL,
    mariano_url          TEXT,
    walmart_url          TEXT,
    stock_on_hand        INTEGER NOT NULL DEFAULT 0,
    reorder_level        INTEGER NOT NULL DEFAULT 0,
    batch_received_at    TEXT,
    expiry_datetime      TEXT,
    days_to_expiry       INTEGER,
    is_perishable        INTEGER NOT NULL DEFAULT 0 CHECK (is_perishable IN (0, 1)),
    units_sold_last_24h  INTEGER NOT NULL DEFAULT 0,
    updated_at           TEXT    NOT NULL
);
"""

# ON CONFLICT(sku_id) DO UPDATE is what makes this script safe to re-run:
# an existing sku_id gets its row refreshed instead of producing a
# duplicate or a UNIQUE constraint error.
UPSERT = """
INSERT INTO products_common (
    sku_id, product_name, category, unit, our_price, cost_price,
    mariano_url, walmart_url, stock_on_hand, reorder_level,
    batch_received_at, expiry_datetime, days_to_expiry, is_perishable,
    units_sold_last_24h, updated_at
) VALUES (
    :sku_id, :product_name, :category, :unit, :our_price, :cost_price,
    :mariano_url, :walmart_url, :stock_on_hand, :reorder_level,
    :batch_received_at, :expiry_datetime, :days_to_expiry, :is_perishable,
    :units_sold_last_24h, strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
)
ON CONFLICT(sku_id) DO UPDATE SET
    product_name         = excluded.product_name,
    category              = excluded.category,
    unit                  = excluded.unit,
    our_price             = excluded.our_price,
    cost_price             = excluded.cost_price,
    mariano_url           = excluded.mariano_url,
    walmart_url           = excluded.walmart_url,
    stock_on_hand         = excluded.stock_on_hand,
    reorder_level         = excluded.reorder_level,
    batch_received_at     = excluded.batch_received_at,
    expiry_datetime       = excluded.expiry_datetime,
    days_to_expiry        = excluded.days_to_expiry,
    is_perishable         = excluded.is_perishable,
    units_sold_last_24h   = excluded.units_sold_last_24h,
    updated_at            = excluded.updated_at;
"""


def _to_bool_int(value: str) -> int:
    return 1 if str(value).strip().lower() in ("true", "1", "yes") else 0


def _to_int_or_none(value):
    value = (value or "").strip()
    return int(value) if value else None


def read_rows(csv_path: Path):
    """Parse the CSV into dicts with correct Python types for sqlite3."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield {
                "sku_id": row["sku_id"].strip(),
                "product_name": row["product_name"].strip(),
                "category": row["category"].strip(),
                "unit": row["unit"].strip(),
                "our_price": float(row["our_price"]),
                "cost_price": float(row["cost_price"]),
                "mariano_url": row.get("mariano_url") or None,
                "walmart_url": row.get("walmart_url") or None,
                "stock_on_hand": _to_int_or_none(row["stock_on_hand"]) or 0,
                "reorder_level": _to_int_or_none(row["reorder_level"]) or 0,
                "batch_received_at": row.get("batch_received_at") or None,
                "expiry_datetime": row.get("expiry_datetime") or None,
                "days_to_expiry": _to_int_or_none(row.get("days_to_expiry")),
                "is_perishable": _to_bool_int(row["is_perishable"]),
                "units_sold_last_24h": _to_int_or_none(row["units_sold_last_24h"]) or 0,
            }


def load(csv_path: Path, db_path: Path) -> None:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(SCHEMA)

        rows = list(read_rows(csv_path))
        if not rows:
            print("No rows found in CSV - nothing to load.")
            return

        conn.executemany(UPSERT, rows)
        conn.commit()

        total = conn.execute("SELECT COUNT(*) FROM products_common").fetchone()[0]
        print(f"Upserted {len(rows)} row(s) from '{csv_path.name}'.")
        print(f"'products_common' now holds {total} row(s) total in '{db_path}'.")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load/refresh the products_common table.")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV, help="Path to source CSV")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite db file")
    args = parser.parse_args()
    load(args.csv, args.db)
