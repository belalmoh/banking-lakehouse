#!/bin/bash

echo "========================================="
echo "BRONZE LAYER - FULL INGESTION"
echo "========================================="

# Function to submit Spark job
submit_job() {
    local job_name=$1
    local script_path=$2
    
    echo ""
    echo "[Running: $job_name]"
    
    docker exec -it banking-spark-master spark-submit \
        --master spark://spark-master:7077 \
        --packages io.delta:delta-core_2.12:2.4.0 \
        --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
        --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
        $script_path
    
    if [ $? -eq 0 ]; then
        echo "✅ $job_name completed successfully"
    else
        echo "❌ $job_name failed"
        exit 1
    fi
}

# Run all Bronze ingestions
submit_job "Customers" "/opt/spark-jobs/bronze/ingest_customers.py"
submit_job "Accounts" "/opt/spark-jobs/bronze/ingest_accounts.py"
submit_job "Transactions" "/opt/spark-jobs/bronze/ingest_transactions.py"
submit_job "AML Alerts" "/opt/spark-jobs/bronze/ingest_aml_alerts.py"

echo ""
echo "========================================="
echo "✅ ALL BRONZE INGESTIONS COMPLETE!"
echo "========================================="