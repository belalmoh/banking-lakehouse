# Architecture Overview

## System Architecture

The Banking Lakehouse follows a **Medallion Architecture** (Bronze → Silver → Gold) pattern, a proven approach for enterprise data lakes that provides clear separation of concerns, data quality guarantees, and audit compliance.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         BANKING LAKEHOUSE                               │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────────────┐  │
│  │  Source   │    │  Kafka   │    │       LAKEHOUSE (MinIO/S3)       │  │
│  │ Systems  │───▶│   CDC    │───▶│                                  │  │
│  │ CSV/JSON │    │          │    │  ┌────────┐ ┌────────┐ ┌──────┐ │  │
│  └──────────┘    └──────────┘    │  │ Bronze │▶│ Silver │▶│ Gold │ │  │
│                                  │  │  (Raw) │ │(Clean) │ │(Biz) │ │  │
│                                  │  └────────┘ └────────┘ └──────┘ │  │
│                                  │       Delta Lake Tables          │  │
│                                  └──────────────────────────────────┘  │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────────┐  │
│  │  Spark   │    │ Airflow  │    │   dbt    │    │ Great            │  │
│  │ Cluster  │    │ Scheduler│    │ Models   │    │ Expectations     │  │
│  │ 1M + 2W  │    │ + Web UI │    │ SQL Txfm │    │ Data Quality     │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────────┘  │
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────────────┐  │
│  │ Jupyter  │    │Postgres  │    │     Governance & Compliance      │  │
│  │ Notebook │    │ Metadata │    │  PII Tagging · Data Residency    │  │
│  │          │    │          │    │  Audit Trails · AML Screening    │  │
│  └──────────┘    └──────────┘    └──────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### Processing Layer
- **Apache Spark 3.4.0**: 1 master + 2 workers (2GB/2 cores each)
- Handles Bronze ingestion, Silver transformations, and Gold aggregations
- Configured with Delta Lake extensions for ACID transactions

### Storage Layer
- **MinIO (S3-compatible)**: Object storage for all Delta Lake tables
- Buckets: `bronze/`, `silver/`, `gold/`, `raw/`
- Path-style access for local development

### Transformation Layer
- **dbt 1.6.0**: SQL-first transformations for Silver and Gold layers
- Incremental models (Silver) with merge strategy
- Table models (Gold) with full refresh
- Custom macros for currency conversion and risk scoring

### Orchestration Layer
- **Apache Airflow 2.7.0**: DAG-based pipeline scheduling
- Main pipeline: hourly Bronze → Silver → Gold
- Quality monitoring: every 6 hours
- Regulatory reporting: monthly

### Quality Layer
- **Great Expectations 0.17.19**: Automated expectation suites
- Bronze: Schema validation, null checks, value ranges
- Silver: Completeness, referential integrity, business rules
- Gold: Aggregation integrity, risk category validation

### Governance Layer
- PII detection and classification
- Data residency tracking (UAE sovereignty)
- AML screening with configurable thresholds
- SHA-256 hashing for regulatory reports
- Full audit trails from source to Gold

## Data Flow

```
1. Source CSV/JSON files
   ↓
2. Bronze Ingestion (PySpark)
   + audit columns (_ingestion_timestamp, _source_file)
   + data classification (_data_classification, _data_residency)
   → Delta Lake append
   ↓
3. Quality Gate (Great Expectations)
   - Null checks, uniqueness, value ranges
   ↓
4. Silver Transformations (dbt incremental merge)
   + deduplication, type casting
   + AML flagging, email validation
   + _cleansed_timestamp
   ↓
5. Gold Aggregations (dbt table)
   - Customer 360: full customer profile
   - AML Summary: alert intelligence
   - Regulatory Report: CBUAE-ready, hashed IDs
   ↓
6. Analytics & Reporting (Jupyter, Airflow)
```

## Network Architecture

All services communicate over a Docker bridge network (`lakehouse-network`):

| Service | Internal Port | External Port |
|---------|-------------|---------------|
| Spark Master | 8080, 7077, 4040 | 8080, 7077, 4040 |
| Spark Worker 1 | — | — |
| Spark Worker 2 | — | — |
| MinIO | 9000, 9001 | 9000, 9001 |
| PostgreSQL | 5432 | 5432 |
| Airflow Webserver | 8080 | 8081 |
| Airflow Scheduler | — | — |
| Jupyter | 8888 | 8888 |
| Zookeeper | 2181 | 2181 |
| Kafka | 9092 | 9092, 29092 |
