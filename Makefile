# =============================================================================
# Banking Lakehouse - Makefile
# =============================================================================

.PHONY: setup start stop clean generate-data test demo \
        logs-spark logs-airflow shell-spark notebook help

DOCKER_COMPOSE = docker compose -f docker/docker-compose.yml --env-file docker/.env
PROJECT_DIR = $(shell pwd)

# ─── Primary Targets ────────────────────────────────────────────────────────

help: ## Show this help message
	@echo "Banking Lakehouse - Available Commands:"
	@echo "──────────────────────────────────────────"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Full environment initialization
	@echo "🚀 Setting up Banking Lakehouse environment..."
	@bash scripts/setup.sh

start: ## Start all Docker services
	@echo "▶️  Starting all services..."
	$(DOCKER_COMPOSE) up -d
	@echo "✅ Services started. Access UIs:"
	@echo "   Spark:   http://localhost:8080"
	@echo "   Airflow: http://localhost:8081"
	@echo "   MinIO:   http://localhost:9001"
	@echo "   Jupyter: http://localhost:8888"

stop: ## Stop all services
	@echo "⏹️  Stopping all services..."
	$(DOCKER_COMPOSE) down

clean: ## Remove all data and reset environment
	@echo "🧹 Cleaning up..."
	$(DOCKER_COMPOSE) down -v --remove-orphans
	rm -rf data/lakehouse/*
	rm -rf data/sample/*
	rm -rf dbt-project/target dbt-project/dbt_packages dbt-project/logs
	rm -rf great_expectations/uncommitted great_expectations/data_docs
	rm -rf spark-warehouse metastore_db
	@echo "✅ Environment cleaned."

generate-data: ## Generate sample banking data
	@echo "📊 Generating sample banking data..."
	python scripts/generate_data.py
	@echo "✅ Sample data generated in data/sample/"

test: ## Run all tests
	@echo "🧪 Running tests..."
	python -m pytest tests/ -v --tb=short
	@echo "✅ Tests completed."

test-unit: ## Run unit tests only
	python -m pytest tests/unit/ -v --tb=short

test-integration: ## Run integration tests only
	python -m pytest tests/integration/ -v --tb=short

demo: ## Execute end-to-end demo pipeline
	@echo "🎬 Running Banking Lakehouse Demo..."
	@bash scripts/run_demo.sh

# ─── Service Logs ────────────────────────────────────────────────────────────

logs-spark: ## View Spark master logs
	$(DOCKER_COMPOSE) logs -f spark-master

logs-airflow: ## View Airflow webserver logs
	$(DOCKER_COMPOSE) logs -f airflow-webserver

logs-kafka: ## View Kafka logs
	$(DOCKER_COMPOSE) logs -f kafka

logs-all: ## View all service logs
	$(DOCKER_COMPOSE) logs -f

# ─── Interactive Shells ──────────────────────────────────────────────────────

shell-spark: ## Open shell in Spark master container
	$(DOCKER_COMPOSE) exec spark-master bash

shell-airflow: ## Open shell in Airflow webserver container
	$(DOCKER_COMPOSE) exec airflow-webserver bash

notebook: ## Open Jupyter Lab URL
	@echo "📓 Jupyter Lab: http://localhost:8888"
	@open http://localhost:8888 2>/dev/null || echo "Open http://localhost:8888 in your browser"

# ─── dbt Commands ────────────────────────────────────────────────────────────

dbt-compile: ## Compile dbt models
	cd dbt-project && dbt compile

dbt-run: ## Run all dbt models
	cd dbt-project && dbt run

dbt-test: ## Run dbt tests
	cd dbt-project && dbt test

dbt-docs: ## Generate dbt documentation
	cd dbt-project && dbt docs generate && dbt docs serve --port 8082

# ─── Status ──────────────────────────────────────────────────────────────────

status: ## Show status of all services
	$(DOCKER_COMPOSE) ps

health: ## Health check all services
	@echo "🏥 Checking service health..."
	@echo "MinIO:    $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:9000/minio/health/live)"
	@echo "Spark:    $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/)"
	@echo "Airflow:  $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8081/health)"
	@echo "Jupyter:  $$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8888/)"
