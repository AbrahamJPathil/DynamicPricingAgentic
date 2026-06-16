"""
view_db.py

Quick inspector for store.db. Prints the schema and contents of every
table, or a single table if you ask for one. No external dependencies.

Usage:
    python view_db.py                  # view every table
    python view_db.py --table products_common
    python view_db.py --db ../store.db --table products_common
"""

import argparse
import sqlite3
from pathlib import Path

DEFAULT_DB = Path(__file__).parent / "store.db"


def get_table_names(conn: sqlite3.Connection) -> list[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
    )
    return [row[0] for row in cur.fetchall()]


def print_table(conn: sqlite3.Connection, table: str) -> None:
    cur = conn.execute(f"SELECT sql FROM sqlite_master WHERE name = ?", (table,))
    schema_row = cur.fetchone()
    print(f"\n{'=' * 70}\nTABLE: {table}\n{'=' * 70}")
    if schema_row:
        print(schema_row[0])

    cur = conn.execute(f"SELECT * FROM {table};")
    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()

    print(f"\n{len(rows)} row(s)\n")
    if not rows:
        return

    # Compute a sensible column width per column, capped so long URLs etc.
    # don't blow out the whole table.
    widths = []
    for i, col in enumerate(columns):
        max_val_len = max((len(str(r[i])) for r in rows), default=0)
        widths.append(min(max(len(col), max_val_len), 40))

    def fmt_row(values):
        cells = []
        for v, w in zip(values, widths):
            text = "" if v is None else str(v)
            if len(text) > w:
                text = text[: w - 1] + "…"
            cells.append(text.ljust(w))
        return " | ".join(cells)

    print(fmt_row(columns))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(fmt_row(row))


def main() -> None:
    parser = argparse.ArgumentParser(description="View the contents of store.db")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite db file")
    parser.add_argument("--table", type=str, default=None, help="Only show this table")
    args = parser.parse_args()

    if not args.db.exists():
        raise FileNotFoundError(f"Database not found: {args.db}")

    conn = sqlite3.connect(args.db)
    try:
        tables = [args.table] if args.table else get_table_names(conn)
        if not tables:
            print("No tables found in this database.")
            return
        for table in tables:
            print_table(conn, table)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
