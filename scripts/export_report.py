#!/usr/bin/env python3
"""
export_report.py
================
Export analytical query results to CSV or Excel.

Ships with several built-in report definitions that query the warehouse
materialized views and marts.  Custom SQL can also be supplied directly.

Usage:
    python scripts/export_report.py --report daily_trade --format xlsx
    python scripts/export_report.py --sql "SELECT * FROM monthly_risk_trends" --output reports/custom.csv
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path

import click
import pandas as pd
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://customs:customs@localhost:5432/customs_dw",
)

# ---- Built-in reports -------------------------------------------------------

REPORTS: dict[str, str] = {
    "daily_trade": """
        SELECT *
        FROM daily_trade_summary
        ORDER BY full_date DESC
    """,
    "monthly_risk": """
        SELECT *
        FROM monthly_risk_trends
        ORDER BY year, month, risk_level
    """,
    "top_risk_importers": """
        SELECT *
        FROM top_risk_importers
        ORDER BY avg_risk_score DESC
        LIMIT 50
    """,
    "commodity_concentration": """
        SELECT *
        FROM commodity_concentration
        ORDER BY total_declared_value DESC
    """,
    "route_analysis": """
        SELECT *
        FROM route_analysis
        ORDER BY total_declared_value DESC
    """,
}


# ---- Helpers ----------------------------------------------------------------

def _run_query(dsn: str, sql: str) -> pd.DataFrame:
    """Execute *sql* and return the result as a DataFrame."""
    conn = psycopg2.connect(dsn)
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    finally:
        conn.close()


def _write_output(df: pd.DataFrame, output_path: str, fmt: str):
    """Write *df* to disk in the requested format."""
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        df.to_csv(output_path, index=False)
    elif fmt in ("xlsx", "excel"):
        df.to_excel(output_path, index=False, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    logger.info("Written %d rows to %s", len(df), output_path)


def _default_output_path(report_name: str, fmt: str) -> str:
    """Generate a timestamped output file path."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    ext = "xlsx" if fmt in ("xlsx", "excel") else "csv"
    return os.path.join("reports", f"{report_name}_{ts}.{ext}")


# ---- CLI --------------------------------------------------------------------

@click.command()
@click.option("--report", "-r", type=click.Choice(list(REPORTS.keys())), help="Built-in report name")
@click.option("--sql", "-s", default=None, help="Custom SQL query (overrides --report)")
@click.option("--format", "-f", "fmt", default="csv", type=click.Choice(["csv", "xlsx"]), help="Output format")
@click.option("--output", "-o", default=None, help="Output file path (auto-generated if omitted)")
@click.option("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
def export(report: str | None, sql: str | None, fmt: str, output: str | None, dsn: str):
    """Export analytical query results to CSV or Excel."""

    if sql:
        report_name = "custom_query"
        query = sql
    elif report:
        report_name = report
        query = REPORTS[report]
    else:
        logger.error("Provide either --report or --sql.")
        sys.exit(1)

    output_path = output or _default_output_path(report_name, fmt)

    logger.info("Running report: %s", report_name)
    df = _run_query(dsn, query)
    logger.info("Query returned %d rows.", len(df))

    if df.empty:
        logger.warning("No data returned — output file will be empty.")

    _write_output(df, output_path, fmt)


if __name__ == "__main__":
    export()
