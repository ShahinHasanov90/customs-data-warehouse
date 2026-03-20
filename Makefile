.PHONY: init-db seed load test query clean help

# ---------- Configuration -----------------------------------------------------
DB_HOST   ?= localhost
DB_PORT   ?= 5432
DB_NAME   ?= customs_dw
DB_USER   ?= customs
DB_PASS   ?= customs
DSN       ?= postgresql://$(DB_USER):$(DB_PASS)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)
PSQL      := PGPASSWORD=$(DB_PASS) psql -h $(DB_HOST) -p $(DB_PORT) -U $(DB_USER) -d $(DB_NAME)

# ---------- Targets -----------------------------------------------------------

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

init-db: ## Start PostgreSQL via Docker and create the schema
	docker compose up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@until $(PSQL) -c '\q' 2>/dev/null; do sleep 1; done
	$(PSQL) -f schema/init.sql
	$(PSQL) -f schema/indexes.sql
	$(PSQL) -f schema/views.sql
	@echo "Schema initialised."

seed: ## Load seed / sample data into dimension and fact tables
	$(PSQL) -f schema/seed_data.sql
	@echo "Seed data loaded."

load: ## Load CSV data using the Python loader
	python scripts/load_data.py --table fact_declarations --file data/declarations.csv --dsn "$(DSN)"

test: ## Run data quality checks
	python scripts/data_quality.py --dsn "$(DSN)" --verbose

query: ## Run example analytical queries
	@echo "\n=== Top Risk Importers ==="
	$(PSQL) -f queries/top_risk_importers.sql
	@echo "\n=== Trade Corridor Analysis ==="
	$(PSQL) -f queries/trade_corridor_analysis.sql
	@echo "\n=== Seasonal Patterns ==="
	$(PSQL) -f queries/seasonal_patterns.sql
	@echo "\n=== Commodity Trends ==="
	$(PSQL) -f queries/commodity_trends.sql

clean: ## Tear down the database container and remove volumes
	docker compose down -v
	@echo "Cleaned up."
