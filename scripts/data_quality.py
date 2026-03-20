#!/usr/bin/env python3
"""
data_quality.py
===============
Automated data quality checks for the customs data warehouse.

Validates:
  - Completeness   : no unexpected NULLs in required columns
  - Uniqueness      : surrogate and natural keys are unique
  - Referential integrity : all foreign keys resolve to dimension tables
  - Range checks   : risk scores in [0,100], values non-negative, etc.

Usage:
    python scripts/data_quality.py
    python scripts/data_quality.py --check completeness
    python scripts/data_quality.py --verbose
"""

import os
import sys
import logging
from dataclasses import dataclass

import click
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


# ---- Data structures --------------------------------------------------------

@dataclass
class CheckResult:
    category: str
    name: str
    passed: bool
    detail: str


# ---- Check definitions ------------------------------------------------------

COMPLETENESS_CHECKS: list[tuple[str, str]] = [
    ("fact_declarations", "declaration_no"),
    ("fact_declarations", "date_key"),
    ("fact_declarations", "importer_key"),
    ("fact_declarations", "exporter_key"),
    ("fact_declarations", "commodity_key"),
    ("fact_declarations", "route_key"),
    ("fact_declarations", "declared_value"),
    ("fact_declarations", "weight_kg"),
    ("fact_declarations", "risk_score"),
    ("fact_declarations", "risk_level"),
    ("dim_importer",      "importer_id"),
    ("dim_importer",      "importer_name"),
    ("dim_exporter",      "exporter_id"),
    ("dim_exporter",      "exporter_name"),
    ("dim_commodity",     "hs_code"),
    ("dim_commodity",     "description"),
    ("dim_date",          "full_date"),
]

UNIQUENESS_CHECKS: list[tuple[str, str]] = [
    ("fact_declarations", "declaration_no"),
    ("dim_importer",      "importer_id"),
    ("dim_exporter",      "exporter_id"),
    ("dim_commodity",     "hs_code"),
    ("dim_customs_post",  "post_code"),
    ("dim_date",          "date_key"),
]

REFERENTIAL_CHECKS: list[tuple[str, str, str, str]] = [
    ("fact_declarations", "date_key",        "dim_date",         "date_key"),
    ("fact_declarations", "importer_key",    "dim_importer",     "importer_key"),
    ("fact_declarations", "exporter_key",    "dim_exporter",     "exporter_key"),
    ("fact_declarations", "commodity_key",   "dim_commodity",    "commodity_key"),
    ("fact_declarations", "route_key",       "dim_route",        "route_key"),
    ("fact_declarations", "customs_post_key","dim_customs_post", "customs_post_key"),
]


# ---- Runners ----------------------------------------------------------------

def _execute_scalar(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def run_completeness(conn) -> list[CheckResult]:
    results = []
    for table, column in COMPLETENESS_CHECKS:
        null_count = _execute_scalar(
            conn,
            f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL",
        )
        passed = null_count == 0
        results.append(CheckResult(
            category="completeness",
            name=f"{table}.{column}",
            passed=passed,
            detail=f"{null_count} NULL(s)" if not passed else "OK",
        ))
    return results


def run_uniqueness(conn) -> list[CheckResult]:
    results = []
    for table, column in UNIQUENESS_CHECKS:
        dup_count = _execute_scalar(
            conn,
            f"""
            SELECT COUNT(*) FROM (
                SELECT {column}
                FROM {table}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            ) dupes
            """,
        )
        passed = dup_count == 0
        results.append(CheckResult(
            category="uniqueness",
            name=f"{table}.{column}",
            passed=passed,
            detail=f"{dup_count} duplicate group(s)" if not passed else "OK",
        ))
    return results


def run_referential(conn) -> list[CheckResult]:
    results = []
    for child_table, child_col, parent_table, parent_col in REFERENTIAL_CHECKS:
        orphan_count = _execute_scalar(
            conn,
            f"""
            SELECT COUNT(*)
            FROM {child_table} c
            LEFT JOIN {parent_table} p ON c.{child_col} = p.{parent_col}
            WHERE p.{parent_col} IS NULL
            """,
        )
        passed = orphan_count == 0
        results.append(CheckResult(
            category="referential",
            name=f"{child_table}.{child_col} -> {parent_table}.{parent_col}",
            passed=passed,
            detail=f"{orphan_count} orphan(s)" if not passed else "OK",
        ))
    return results


def run_range_checks(conn) -> list[CheckResult]:
    results = []

    # Risk score in [0, 100]
    bad_risk = _execute_scalar(
        conn,
        "SELECT COUNT(*) FROM fact_declarations WHERE risk_score < 0 OR risk_score > 100",
    )
    results.append(CheckResult(
        category="range",
        name="fact_declarations.risk_score IN [0,100]",
        passed=bad_risk == 0,
        detail=f"{bad_risk} out-of-range" if bad_risk else "OK",
    ))

    # Declared value non-negative
    bad_value = _execute_scalar(
        conn,
        "SELECT COUNT(*) FROM fact_declarations WHERE declared_value < 0",
    )
    results.append(CheckResult(
        category="range",
        name="fact_declarations.declared_value >= 0",
        passed=bad_value == 0,
        detail=f"{bad_value} negative" if bad_value else "OK",
    ))

    # Weight non-negative
    bad_weight = _execute_scalar(
        conn,
        "SELECT COUNT(*) FROM fact_declarations WHERE weight_kg < 0",
    )
    results.append(CheckResult(
        category="range",
        name="fact_declarations.weight_kg >= 0",
        passed=bad_weight == 0,
        detail=f"{bad_weight} negative" if bad_weight else "OK",
    ))

    # Valid risk levels
    bad_level = _execute_scalar(
        conn,
        "SELECT COUNT(*) FROM fact_declarations WHERE risk_level NOT IN ('LOW','MEDIUM','HIGH','CRITICAL')",
    )
    results.append(CheckResult(
        category="range",
        name="fact_declarations.risk_level valid enum",
        passed=bad_level == 0,
        detail=f"{bad_level} invalid" if bad_level else "OK",
    ))

    return results


# ---- Main -------------------------------------------------------------------

CHECK_RUNNERS = {
    "completeness": run_completeness,
    "uniqueness": run_uniqueness,
    "referential": run_referential,
    "range": run_range_checks,
}


@click.command()
@click.option("--check", "-c", "checks", multiple=True,
              type=click.Choice(list(CHECK_RUNNERS.keys())),
              help="Run specific check categories (default: all)")
@click.option("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
@click.option("--verbose", "-v", is_flag=True, default=False, help="Show passing checks too")
def main(checks: tuple[str, ...], dsn: str, verbose: bool):
    """Run data quality checks against the customs data warehouse."""

    selected = checks or tuple(CHECK_RUNNERS.keys())
    conn = psycopg2.connect(dsn)

    all_results: list[CheckResult] = []

    try:
        for category in selected:
            logger.info("Running %s checks ...", category)
            results = CHECK_RUNNERS[category](conn)
            all_results.extend(results)
    finally:
        conn.close()

    # Report
    passed = sum(1 for r in all_results if r.passed)
    failed = sum(1 for r in all_results if not r.passed)
    total = len(all_results)

    print("\n" + "=" * 70)
    print("DATA QUALITY REPORT")
    print("=" * 70)

    for r in all_results:
        if not r.passed or verbose:
            status = "PASS" if r.passed else "FAIL"
            print(f"  [{status}] {r.category:15s} | {r.name:50s} | {r.detail}")

    print("-" * 70)
    print(f"  Total: {total}  |  Passed: {passed}  |  Failed: {failed}")
    print("=" * 70 + "\n")

    if failed > 0:
        logger.error("%d check(s) FAILED.", failed)
        sys.exit(1)
    else:
        logger.info("All %d checks PASSED.", total)


if __name__ == "__main__":
    main()
