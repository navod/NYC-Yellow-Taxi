# Common tasks. Run `make` or `make help` to list them.
.DEFAULT_GOAL := help
COMPOSE := docker compose

.PHONY: help setup up down restart ps logs test lint docs dashboard unpause clean

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

setup: ## One-shot setup: venv, data download, start all services
	./scripts/setup.sh

up: ## Build and start Postgres, Airflow and Superset
	$(COMPOSE) up --detach --build --wait

down: ## Stop all services (keeps the database volume)
	$(COMPOSE) down

restart: down up ## Recreate all services

ps: ## Show service status
	$(COMPOSE) ps

logs: ## Follow logs of all services
	$(COMPOSE) logs --follow --tail=100

test: ## Run the test suite
	./scripts/run_tests.sh

lint: ## Syntax-check shell scripts (and shellcheck them if installed)
	bash -n scripts/*.sh
	@command -v shellcheck >/dev/null && shellcheck scripts/*.sh || echo "shellcheck not installed, skipped"

docs: ## Regenerate docs/schema/data_dictionary.csv from sql/schema.sql
	.venv/bin/python scripts/generate_data_dictionary.py

unpause: ## Unpause the ETL DAG so the scheduler runs it
	$(COMPOSE) exec airflow-scheduler airflow dags unpause nyc_taxi_etl

dashboard: ## Create or update the Superset dashboard (idempotent)
	$(COMPOSE) exec -T superset python /app/bootstrap/bootstrap_dashboard.py

clean: ## Remove Python caches
	find . -name __pycache__ -not -path './.venv/*' -prune -exec rm -rf {} +
	rm -rf .pytest_cache
