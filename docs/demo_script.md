# Interview Demo Script

## 5-Minute Walkthrough

### 1. Architecture Overview (1 minute)

**Show**: README.md architecture diagram

**Say**: "This is a production-ready banking lakehouse built on the Medallion Architecture. Source banking data flows through three layers — Bronze for raw ingestion with audit trails, Silver for cleansed and validated data, and Gold for business aggregations like Customer 360 and regulatory reports. Everything runs locally on Docker with Spark, Airflow, Delta Lake, and dbt."

**Key Points**:
- Medallion Architecture (Bronze → Silver → Gold)
- Delta Lake for ACID transactions and time travel
- dbt for SQL-first transformations
- Airflow for production orchestration
- Full CBUAE compliance (AML, KYC, data residency)

---

### 2. Data Generation (30 seconds)

```bash
make generate-data
```

**Say**: "I've generated realistic banking data using Faker — 10,000 customers, 20,000 accounts, 100,000 transactions, and 5,000 AML alerts. The data includes UAE-specific features like AED currency, CBUAE-compliant fields, and realistic customer segments."

---

### 3. Pipeline Execution (2 minutes)

```bash
make demo
```

**Say**: "The pipeline runs through three stages:"

**Bronze** (show Spark UI at http://localhost:8080):
- "Raw data ingested with audit columns — ingestion timestamp, source file, data classification, and data residency tracking."

**Silver** (show MinIO at http://localhost:9001):
- "dbt applies incremental merge transformations — deduplication, type validation, AML flagging. Transactions over AED 50K are automatically flagged."

**Gold** (show Jupyter notebook 04):
- "Three business views: Customer 360 joining all entities, AML Summary for compliance, and a CBUAE Regulatory Report with SHA-256 hashed customer IDs for privacy."

---

### 4. Explore Results (1 minute)

**Show Jupyter** (http://localhost:8888 → `05_lineage_demo.ipynb`):
- "Every record is traceable from Gold back to the original source file. Delta Lake time travel lets us query any historical version."

**Show Airflow** (http://localhost:8081):
- "The DAG runs hourly with quality gates between each layer. If Bronze quality checks fail, Silver doesn't execute."

---

### 5. Quality & Compliance (30 seconds)

**Key Features to Highlight**:
- "Data classification: every record tagged PII, CONFIDENTIAL, or PUBLIC"
- "AML screening: configurable thresholds, alert lifecycle tracking"
- "Audit trails: full lineage from source to report"
- "Data residency: UAE sovereignty tracking"
- "Composite risk scoring: weighted KYC (40%) + AML alerts (35%) + flagged transactions (25%)"

---

## Interview Talking Points

### Why Delta Lake?
"Delta Lake gives us ACID transactions on object storage — critical for banking. Schema evolution handles upstream changes gracefully. Time travel supports regulatory audits. And Change Data Feed enables downstream CDC."

### Why dbt?
"dbt makes transformations testable, documented, and version-controllable. The declarative SQL approach is maintainable by both data engineers and analysts. Built-in testing catches data quality issues before they reach Gold."

### Why Medallion Architecture?
"The three-layer approach preserves raw data for audit compliance, enables iterative quality improvement, and allows multiple Gold views without reprocessing. It's the de facto standard for enterprise lakehouses."

### Consultancy Mindset
"As a SentraAI consultant, I'd approach ADCB's data challenges by: (1) understanding their regulatory requirements first, (2) implementing production patterns that scale, (3) enabling self-service analytics through documented Gold views, and (4) building quality gates that prevent bad data from reaching business users."
