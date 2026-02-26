# 🏦 Banking Lakehouse

> Production-ready banking data lakehouse demonstrating enterprise data engineering patterns with the Medallion Architecture.

[![Spark](https://img.shields.io/badge/Apache%20Spark-3.4.0-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-2.4.0-blue)](https://delta.io/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.0-green)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.6.0-red)](https://www.getdbt.com/)

---

## 🏗️ Architecture

```
┌──────────────┐     ┌──────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│Source Systems │────▶│ Kafka CDC│────▶│ 🥉 Bronze    │────▶│ 🥈 Silver    │────▶│ 🥇 Gold      │
│  CSV / JSON  │     │          │     │   (Raw)      │     │  (Cleansed)  │     │  (Business)  │
└──────────────┘     └──────────┘     └──────┬───────┘     └──────┬───────┘     └──────┬───────┘
                                             │                    │                    │
                                       Audit Trails         Data Quality        Regulatory Reports
                                       Schema Evolution     AML Flagging        Customer 360
                                       Data Classification  Deduplication       AML Summary
```

### Medallion Architecture

| Layer | Purpose | Storage | Materialization |
|-------|---------|---------|-----------------|
| **Bronze** | Raw ingestion with audit trails | Delta Lake on MinIO (S3) | Append-only |
| **Silver** | Cleansed, deduplicated, validated | Delta Lake on MinIO (S3) | Incremental merge |
| **Gold** | Business aggregations & reports | Delta Lake on MinIO (S3) | Full refresh |

---

## 🚀 Quick Start

```bash
# 1. Clone and enter project
git clone <repo-url> && cd banking-lakehouse

# 2. Generate sample data (10K customers, 100K transactions)
make generate-data

# 3. Start all services
make setup

# 4. Run the full pipeline demo
make demo
```

---

## 📊 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Processing** | Apache Spark 3.4.0 | Distributed data processing |
| **Storage** | Delta Lake 2.4.0 | ACID transactions, time travel |
| **Object Store** | MinIO (S3-compatible) | Lakehouse file storage |
| **Transformations** | dbt 1.6.0 | SQL-based Silver/Gold models |
| **Orchestration** | Apache Airflow 2.7.0 | Pipeline scheduling & monitoring |
| **Data Quality** | Great Expectations 0.17.19 | Automated validation |
| **Streaming** | Apache Kafka | CDC event simulation |
| **Metadata** | PostgreSQL 15 | Airflow & catalog metadata |
| **Exploration** | Jupyter Lab | Interactive notebooks |
| **Containers** | Docker Compose | Local environment orchestration |

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Spark UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |
| MinIO Console | http://localhost:9001 | admin / admin123 |
| Jupyter Lab | http://localhost:8888 | No password |

---

## 📁 Project Structure

```
banking-lakehouse/
├── docker/                  # Docker Compose & service configs
│   ├── docker-compose.yml   # 9 services (Spark, Airflow, MinIO, Kafka, etc.)
│   ├── spark/               # Spark + Delta Lake Dockerfile
│   └── airflow/             # Airflow Dockerfile
├── spark-jobs/              # PySpark pipeline jobs
│   ├── bronze_ingestion.py  # Raw → Bronze with audit trails
│   ├── silver_transformations.py  # Bronze → Silver cleansing
│   ├── gold_aggregations.py # Silver → Gold business views
│   ├── data_quality_checks.py     # Quality validation
│   ├── cdc_consumer.py      # Kafka CDC consumer
│   └── utils/               # Spark session, Delta helpers, classification
├── dbt-project/             # SQL transformations
│   ├── models/silver/       # 4 incremental cleansing models
│   ├── models/gold/         # 3 business aggregation models
│   └── macros/              # Currency conversion, risk scoring
├── airflow-dags/            # Orchestration DAGs
│   ├── banking_lakehouse_pipeline.py  # Main hourly pipeline
│   ├── data_quality_monitoring.py     # Quality checks (6h)
│   └── regulatory_reporting.py        # CBUAE monthly reports
├── scripts/                 # Setup & demo scripts
├── data/                    # Sample data & lakehouse storage
├── notebooks/               # 5 Jupyter exploration notebooks
├── tests/                   # Unit & integration tests
├── great_expectations/      # Data quality suites
└── docs/                    # Architecture & compliance docs
```

---

## 🏦 Banking Domain Features

### Anti-Money Laundering (AML)
- Transactions > AED 50,000 flagged automatically
- International transfers to high-risk countries detected
- High-risk customer scoring (risk_score > 80)
- AML Summary Gold table with resolution tracking

### Know Your Customer (KYC)
- Customer verification status tracking (VERIFIED/PENDING/REJECTED)
- Customer segmentation (RETAIL/CORPORATE/PRIVATE_BANKING)
- Composite risk scoring combining KYC + AML + transaction patterns

### CBUAE Regulatory Compliance
- Privacy-preserving customer identifiers (SHA-256 hashed)
- Data residency tracking (UAE sovereignty)
- Data classification (PII/CONFIDENTIAL/PUBLIC)
- Monthly regulatory reports with audit trails
- All timestamps in UTC

---

## 🔄 Data Pipeline

### Bronze Ingestion (`spark-jobs/bronze_ingestion.py`)
- Reads CSV/JSON source files
- Adds audit columns: `_ingestion_timestamp`, `_source_file`, `_batch_id`
- Adds governance: `_data_classification`, `_data_residency`, `_pii_fields`
- Writes to Delta Lake with schema evolution
- Idempotent, append-only design

### Silver Transformations (dbt + PySpark)
- Deduplication by primary key (keep latest)
- Type casting and null handling
- Business rule validation (positive amounts, valid currencies)
- AML flagging logic
- Incremental merge strategy

### Gold Aggregations (dbt + PySpark)
- **Customer 360**: Joined view across all entities with composite risk
- **AML Summary**: Alert intelligence by customer and type
- **Regulatory Report**: CBUAE-ready with hashed identifiers

---

## 🧪 Testing

```bash
# Run all tests
make test

# Unit tests only
make test-unit

# Integration tests only
make test-integration

# dbt tests
make dbt-test
```

### Test Coverage
- **Unit**: Bronze ingestion, Delta operations, data classification, AML flagging
- **Integration**: End-to-end pipeline, dbt model validation, lineage preservation
- **Data Quality**: Great Expectations suites for Bronze, Silver, and Gold layers

---

## 🎬 Demo Walkthrough (5 minutes)

1. **Architecture Overview** (1 min) — Show Docker services, explain Medallion pattern
2. **Data Generation** (30 sec) — `make generate-data` → 135K+ records
3. **Pipeline Execution** (2 min) — `make demo` → Bronze → Silver → Gold
4. **Explore Results** (1 min) — Jupyter notebooks, MinIO console
5. **Quality & Compliance** (30 sec) — AML flags, data classification, audit trails

---

## 🔑 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Delta Lake** over Parquet | ACID transactions, time travel, schema evolution |
| **dbt** for transformations | SQL-first, testable, documented, industry standard |
| **MinIO** over HDFS | S3-compatible, lightweight, laptop-friendly |
| **Airflow** for orchestration | Production-grade scheduling, DAG visualization |
| **Great Expectations** | Automated data quality with data docs |
| **Incremental merge** (Silver) | Efficient processing, exactly-once semantics |
| **Full refresh** (Gold) | Consistent aggregations, simpler logic |

---

## 🛠️ Troubleshooting

| Issue | Solution |
|-------|----------|
| Docker services won't start | Check Docker memory (≥8GB), run `make clean && make start` |
| Spark job fails | Check logs: `make logs-spark`, verify MinIO is healthy |
| Airflow DAG not visible | Wait 30s for scheduler scan, check `make logs-airflow` |
| MinIO connection refused | Verify port 9000 is free, check `docker ps` |
| dbt compilation error | Run `cd dbt-project && dbt debug` for connection details |
| Jupyter kernel dies | Reduce data volume, check Spark worker memory |

---

## 📋 Resource Requirements

- **RAM**: 8GB minimum (16GB recommended)
- **Disk**: 5GB for Docker images + data
- **CPU**: 4+ cores recommended
- **Docker**: Docker Desktop with Compose V2
- **Ports**: 8080, 8081, 8888, 9000, 9001, 9092, 2181, 5432, 7077

---

## 📝 License

This project is built for interview demonstration purposes.
