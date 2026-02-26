#!/bin/bash
# =============================================================================
# Banking Lakehouse Demo Script
# Executes the end-to-end pipeline: Bronze → Silver → Gold
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DOCKER_COMPOSE="docker compose -f ${PROJECT_DIR}/docker/docker-compose.yml --env-file ${PROJECT_DIR}/docker/.env"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║      Banking Lakehouse — End-to-End Demo                     ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ─── Step 1: Bronze Ingestion ───────────────────────────────────────────────
echo "🥉 Step 1/5: Bronze Layer Ingestion"
echo "  Ingesting raw CSV data into Delta Lake Bronze tables..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    /opt/spark-jobs/bronze_ingestion.py
echo "  ✅ Bronze ingestion complete"
echo ""

# ─── Step 2: Data Quality Checks (Bronze) ───────────────────────────────────
echo "🔍 Step 2/5: Data Quality Validation (Bronze)"
echo "  Running quality checks on Bronze layer..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/data_quality_checks.py
echo "  ✅ Quality checks complete"
echo ""

# ─── Step 3: Silver Transformations ─────────────────────────────────────────
echo "🥈 Step 3/5: Silver Layer Transformations"
echo "  Running PySpark cleansing transformations..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/silver_transformations.py
echo "  ✅ Silver transformations complete"
echo ""

# ─── Step 4: Gold Aggregations ──────────────────────────────────────────────
echo "🥇 Step 4/5: Gold Layer Aggregations"
echo "  Building business-level views (Customer 360, AML, Regulatory)..."
${DOCKER_COMPOSE} exec -T spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    /opt/spark-jobs/gold_aggregations.py
echo "  ✅ Gold aggregations complete"
echo ""

# ─── Step 5: Summary ────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║                    Demo Complete! 🎉                         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║   Pipeline: Bronze → Silver → Gold ✅                       ║"
echo "║                                                              ║"
echo "║   Gold Tables Created:                                       ║"
echo "║     • Customer 360 (10K customers with full profiles)        ║"
echo "║     • AML Summary  (alert intelligence by customer)          ║"
echo "║     • Regulatory Report (CBUAE-ready, hashed IDs)            ║"
echo "║                                                              ║"
echo "║   Explore Results:                                           ║"
echo "║     Spark UI:    http://localhost:8080                       ║"
echo "║     Airflow UI:  http://localhost:8081  (admin/admin)        ║"
echo "║     MinIO:       http://localhost:9001  (admin/admin123)     ║"
echo "║     Jupyter Lab: http://localhost:8888                       ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
