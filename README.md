# Customs Data Warehouse

[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Data warehouse schema and transformation toolkit for customs trade analytics. Star schema design optimized for trade flow analysis, risk scoring aggregation, and compliance reporting. Includes dbt models, data quality tests, and materialized views for common analytical patterns.

## Architecture

```
            +------------------+
            | fact_declarations|
            +--------+---------+
                     |
       +------+------+------+------+------+
       |      |      |      |      |      |
  dim_date  dim_   dim_   dim_    dim_   dim_
          importer exporter commodity route customs_post
```

## Project Structure

```
.
├── schema/
│   ├── init.sql            # Star schema DDL (facts + dimensions)
│   ├── views.sql           # Analytical materialized views
│   ├── indexes.sql         # Performance indexes
│   └── seed_data.sql       # Synthetic seed data
├── dbt/
│   ├── dbt_project.yml     # dbt project configuration
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_declarations.sql
│   │   ├── marts/
│   │   │   ├── fct_daily_trade.sql
│   │   │   ├── fct_risk_summary.sql
│   │   │   └── dim_commodity_enriched.sql
│   │   └── schema.yml
│   └── tests/
│       ├── assert_positive_values.sql
│       └── assert_valid_risk_scores.sql
├── scripts/
│   ├── load_data.py        # CSV → warehouse loader
│   ├── export_report.py    # Analytical report exporter
│   └── data_quality.py     # Data quality checks
├── queries/
│   ├── top_risk_importers.sql
│   ├── trade_corridor_analysis.sql
│   ├── seasonal_patterns.sql
│   └── commodity_trends.sql
├── docker-compose.yml      # PostgreSQL 15
├── Makefile
├── requirements.txt
└── LICENSE
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.10+
- dbt-core with dbt-postgres adapter

### Setup

```bash
# Start PostgreSQL
make init-db

# Initialize schema and seed data
make seed

# Load data from CSV files
make load

# Run dbt transformations
cd dbt && dbt run

# Run data quality tests
make test
```

### Analytical Queries

```bash
# Run pre-built analytical queries
make query

# Export reports to CSV/Excel
python scripts/export_report.py --format xlsx --output reports/
```

## Key Features

- **Star Schema Design** -- Fact and dimension tables optimized for analytical workloads
- **dbt Transformations** -- Staging models, business-logic marts, and enriched dimensions
- **Data Quality** -- Automated checks for completeness, uniqueness, and referential integrity
- **Materialized Views** -- Pre-aggregated summaries for daily trade, risk trends, and route analysis
- **Performance Indexes** -- Covering indexes for common query patterns
- **Reporting Toolkit** -- Python scripts for data loading and report generation

## License

MIT License. See [LICENSE](LICENSE) for details.
