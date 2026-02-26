#!/bin/bash
# =============================================================================
# Initialize Apache Airflow for Banking Lakehouse
# =============================================================================

set -e

DOCKER_COMPOSE="docker compose -f docker/docker-compose.yml --env-file docker/.env"

echo "🌪️  Initializing Airflow..."

# Initialize database
echo "  Migrating Airflow database..."
${DOCKER_COMPOSE} exec -T airflow-webserver airflow db migrate

# Create admin user
echo "  Creating admin user..."
${DOCKER_COMPOSE} exec -T airflow-webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@lakehouse.local || true

# List DAGs
echo "  Verifying DAGs..."
${DOCKER_COMPOSE} exec -T airflow-webserver airflow dags list

echo ""
echo "✅ Airflow initialized"
echo "  UI: http://localhost:8081"
echo "  Credentials: admin / admin"
