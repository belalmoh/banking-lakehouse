#!/bin/bash
# =============================================================================
# Banking Lakehouse - Environment Setup Script
# Initializes the complete environment from scratch
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE="docker compose -f ${PROJECT_DIR}/docker/docker-compose.yml --env-file ${PROJECT_DIR}/docker/.env"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║      Banking Lakehouse — Environment Setup                   ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─── Step 1: Create Data Directories ────────────────────────────────────────
echo "📁 Step 1/7: Creating data directories..."
mkdir -p "${PROJECT_DIR}/data/sample"
mkdir -p "${PROJECT_DIR}/data/lakehouse/bronze/customers"
mkdir -p "${PROJECT_DIR}/data/lakehouse/bronze/accounts"
mkdir -p "${PROJECT_DIR}/data/lakehouse/bronze/transactions"
mkdir -p "${PROJECT_DIR}/data/lakehouse/bronze/aml_alerts"
mkdir -p "${PROJECT_DIR}/data/lakehouse/silver/customers_cleansed"
mkdir -p "${PROJECT_DIR}/data/lakehouse/silver/accounts_cleansed"
mkdir -p "${PROJECT_DIR}/data/lakehouse/silver/transactions_cleansed"
mkdir -p "${PROJECT_DIR}/data/lakehouse/silver/aml_alerts_cleansed"
mkdir -p "${PROJECT_DIR}/data/lakehouse/gold/customer_360"
mkdir -p "${PROJECT_DIR}/data/lakehouse/gold/aml_summary"
mkdir -p "${PROJECT_DIR}/data/lakehouse/gold/regulatory_reports"
echo "  ✅ Data directories created"
echo ""

# ─── Step 2: Generate Sample Data ───────────────────────────────────────────
echo "📊 Step 2/7: Generating sample banking data..."
cd "${PROJECT_DIR}"
python scripts/generate_data.py
echo "  ✅ Sample data generated"
echo ""

# ─── Step 3: Build Docker Images ────────────────────────────────────────────
echo "🐳 Step 3/7: Building Docker images..."
${DOCKER_COMPOSE} build
echo "  ✅ Docker images built"
echo ""

# ─── Step 4: Start Docker Services ──────────────────────────────────────────
echo "▶️  Step 4/7: Starting Docker services..."
${DOCKER_COMPOSE} up -d
echo "  ✅ Docker services started"
echo ""

# ─── Step 5: Wait for Services to be Healthy ────────────────────────────────
echo "⏳ Step 5/7: Waiting for services to be healthy..."

# Wait for MinIO
echo "  Waiting for MinIO..."
for i in {1..30}; do
    if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "  ✅ MinIO is healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ❌ MinIO failed to start"
        exit 1
    fi
    sleep 2
done

# Wait for PostgreSQL
echo "  Waiting for PostgreSQL..."
for i in {1..30}; do
    if ${DOCKER_COMPOSE} exec -T postgres pg_isready -U airflow > /dev/null 2>&1; then
        echo "  ✅ PostgreSQL is healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ❌ PostgreSQL failed to start"
        exit 1
    fi
    sleep 2
done

# Wait for Spark Master
echo "  Waiting for Spark Master..."
for i in {1..30}; do
    if curl -sf http://localhost:8080/ > /dev/null 2>&1; then
        echo "  ✅ Spark Master is healthy"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "  ❌ Spark Master failed to start"
        exit 1
    fi
    sleep 2
done

# Wait for Airflow
echo "  Waiting for Airflow (this may take a minute)..."
for i in {1..60}; do
    if curl -sf http://localhost:8081/health > /dev/null 2>&1; then
        echo "  ✅ Airflow is healthy"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "  ⚠️  Airflow may still be initializing..."
    fi
    sleep 3
done
echo ""

# ─── Step 6: Initialize MinIO Buckets ───────────────────────────────────────
echo "🪣 Step 6/7: MinIO buckets initialized by init container"
echo "  ✅ Buckets: bronze, silver, gold, raw"
echo ""

# ─── Step 7: Smoke Tests ────────────────────────────────────────────────────
echo "🧪 Step 7/7: Running smoke tests..."

TESTS_PASSED=0
TESTS_TOTAL=4

# Test MinIO
if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "  ✅ MinIO: accessible"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "  ❌ MinIO: not accessible"
fi

# Test Spark
if curl -sf http://localhost:8080/ > /dev/null 2>&1; then
    echo "  ✅ Spark UI: accessible"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "  ❌ Spark UI: not accessible"
fi

# Test Airflow
if curl -sf http://localhost:8081/health > /dev/null 2>&1; then
    echo "  ✅ Airflow: accessible"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "  ❌ Airflow: not accessible"
fi

# Test data files
if [ -f "${PROJECT_DIR}/data/sample/customers.csv" ]; then
    CUST_COUNT=$(wc -l < "${PROJECT_DIR}/data/sample/customers.csv" | tr -d ' ')
    echo "  ✅ Data files: present (customers: ${CUST_COUNT} lines)"
    TESTS_PASSED=$((TESTS_PASSED + 1))
else
    echo "  ❌ Data files: not found"
fi

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║   Setup Complete — ${TESTS_PASSED}/${TESTS_TOTAL} smoke tests passed                   ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║   🌐 Service URLs:                                          ║"
echo "║      Spark UI:    http://localhost:8080                      ║"
echo "║      Airflow UI:  http://localhost:8081  (admin/admin)       ║"
echo "║      MinIO UI:    http://localhost:9001  (admin/admin123)    ║"
echo "║      Jupyter Lab: http://localhost:8888                      ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
