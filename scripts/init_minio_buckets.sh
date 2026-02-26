#!/bin/bash
# =============================================================================
# Initialize MinIO Buckets for Banking Lakehouse
# =============================================================================

set -e

echo "🪣 Initializing MinIO buckets..."

# Wait for MinIO to be ready
for i in {1..30}; do
    if curl -sf http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        break
    fi
    echo "  Waiting for MinIO... ($i/30)"
    sleep 2
done

# Configure MinIO client
mc alias set lakehouse http://localhost:9000 admin admin123

# Create buckets
mc mb --ignore-existing lakehouse/bronze
mc mb --ignore-existing lakehouse/silver
mc mb --ignore-existing lakehouse/gold
mc mb --ignore-existing lakehouse/raw

echo "✅ MinIO buckets created:"
mc ls lakehouse/
