#!/usr/bin/env python3
"""
load_data.py
============
Load CSV files into the customs data warehouse.

Supports loading into any dimension or fact table. Automatically maps CSV
columns to table columns and performs basic validation before insertion.

Usage:
    python scripts/load_data.py --table fact_declarations --file data/declarations.csv
    python scripts/load_data.py --table dim_importer --file data/importers.csv --truncate
"""

import os
import sys
import logging

import click
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---- Defaults ---------------------------------------------------------------

DEFAULT_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://customs:customs@localhost:5432/customs_dw",
)

VALID_TABLES = [
    "dim_date",
    "dim_importer",
    "dim_exporter",
    "dim_commodity",
    "dim_route",
    "dim_customs_post",
    "fact_declarations",
]


# ---- Helpers ----------------------------------------------------------------

def _get_connection(dsn: str):
    """Return a new psycopg2 connection."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def _get_table_columns(conn, table_name: str) -> list[str]:
    """Retrieve column names for *table_name* from information_schema."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def _validate_dataframe(df: pd.DataFrame, table_columns: list[str], table_name: str):
    """Warn about column mismatches between the CSV and the target table."""
    csv_cols = set(df.columns)
    tbl_cols = set(table_columns)

    extra = csv_cols - tbl_cols
    missing = tbl_cols - csv_cols

    if extra:
        logger.warning("CSV has columns not in %s: %s (will be ignored)", table_name, extra)
    if missing:
        logger.info("Table %s columns not in CSV (will use defaults): %s", table_name, missing)


# ---- Main -------------------------------------------------------------------

@click.command()
@click.option("--table", "-t", required=True, type=click.Choice(VALID_TABLES), help="Target table name")
@click.option("--file", "-f", "filepath", required=True, type=click.Path(exists=True), help="Path to CSV file")
@click.option("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
@click.option("--truncate", is_flag=True, default=False, help="Truncate table before loading")
@click.option("--batch-size", default=1000, type=int, help="Rows per INSERT batch")
@click.option("--dry-run", is_flag=True, default=False, help="Validate only — do not write")
def load(table: str, filepath: str, dsn: str, truncate: bool, batch_size: int, dry_run: bool):
    """Load a CSV file into the customs data warehouse."""

    logger.info("Reading %s ...", filepath)
    df = pd.read_csv(filepath)
    logger.info("Read %d rows, %d columns.", len(df), len(df.columns))

    if df.empty:
        logger.warning("CSV is empty — nothing to load.")
        sys.exit(0)

    conn = _get_connection(dsn)

    try:
        table_columns = _get_table_columns(conn, table)
        if not table_columns:
            logger.error("Table '%s' not found or has no columns.", table)
            sys.exit(1)

        _validate_dataframe(df, table_columns, table)

        # Keep only columns that exist in both CSV and table
        load_columns = [c for c in df.columns if c in table_columns]
        df = df[load_columns]

        if dry_run:
            logger.info("[DRY RUN] Would load %d rows into %s (%s).", len(df), table, ", ".join(load_columns))
            return

        with conn.cursor() as cur:
            if truncate:
                logger.info("Truncating %s ...", table)
                cur.execute(f"TRUNCATE TABLE {table} CASCADE")

            cols_str = ", ".join(load_columns)
            template = f"({', '.join(['%s'] * len(load_columns))})"

            logger.info("Inserting %d rows into %s ...", len(df), table)

            for start in range(0, len(df), batch_size):
                batch = df.iloc[start : start + batch_size]
                values = [tuple(row) for row in batch.itertuples(index=False, name=None)]
                execute_values(
                    cur,
                    f"INSERT INTO {table} ({cols_str}) VALUES %s ON CONFLICT DO NOTHING",
                    values,
                    template=template,
                )
                logger.info("  ... inserted batch %d–%d", start + 1, start + len(batch))

        conn.commit()
        logger.info("Successfully loaded %d rows into %s.", len(df), table)

    except Exception:
        conn.rollback()
        logger.exception("Load failed — transaction rolled back.")
        sys.exit(1)

    finally:
        conn.close()


if __name__ == "__main__":
    load()
