#!/bin/bash

echo "========================================="
echo "SILVER LAYER - dbt TRANSFORMATION"
echo "========================================="

cd dbt_banking_lakehouse

# Test source connections
echo ""
echo "[1/4] Testing source connections..."
dbt test --select source:bronze

# Run Silver models
echo ""
echo "[2/4] Running Silver layer transformations..."
dbt run --select silver.*

# Test Silver models
echo ""
echo "[3/4] Testing Silver layer data quality..."
dbt test --select silver.*

# Generate documentation
echo ""
echo "[4/4] Generating documentation..."
dbt docs generate

echo ""
echo "========================================="
echo "✅ SILVER LAYER COMPLETE!"
echo "========================================="
echo ""
echo "View documentation: dbt docs serve"