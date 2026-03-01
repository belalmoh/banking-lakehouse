#!/bin/bash

echo "========================================="
echo "GOLD LAYER - dbt AGGREGATIONS"
echo "========================================="

cd dbt_banking_lakehouse

# Run Gold models
echo ""
echo "[1/3] Running Gold layer models..."
dbt run --select gold.*

# Test Gold models
echo ""
echo "[2/3] Testing Gold layer..."
dbt test --select gold.*

# Generate documentation
echo ""
echo "[3/3] Generating documentation..."
dbt docs generate

echo ""
echo "========================================="
echo "✅ GOLD LAYER COMPLETE!"
echo "========================================="
echo ""
echo "Models created:"
echo "  - customer_360"
echo "  - aml_compliance_dashboard"
echo "  - cbuae_monthly_report"
echo "  - ml_features_churn_prediction"
echo ""
echo "View docs: dbt docs serve --port 8080"