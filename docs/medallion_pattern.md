# Medallion Architecture Pattern

## What is Medallion Architecture?

The Medallion Architecture (also called Multi-Hop Architecture) is a data design pattern used to logically organize data in a lakehouse. It progressively improves data quality and structure across three layers:

```
Bronze (Raw) → Silver (Validated) → Gold (Business-Ready)
```

## Why Medallion for Banking?

Banking data has strict requirements:
- **Auditability**: Regulators require full data provenance
- **Data Quality**: Financial data must be accurate and complete
- **Compliance**: CBUAE mandates data residency and classification
- **Reproducibility**: Any report must be recreatable from source

The Medallion Architecture satisfies all of these by preserving raw data (Bronze), applying validated transformations (Silver), and producing trusted business views (Gold).

## Layer Details

### 🥉 Bronze Layer — "Land and Store"
- **Purpose**: Faithful copy of source data
- **Transformations**: None (raw data preserved)
- **Additions**: Audit metadata only
- **Write Pattern**: Append-only (immutable)
- **Schema**: Matches source; schema evolution enabled
- **Retention**: Indefinite (regulatory requirement)

**Banking Example**: Raw CSV extracts from core banking system loaded as-is.

### 🥈 Silver Layer — "Validate and Conform"
- **Purpose**: Cleansed, deduplicated, conformed data
- **Transformations**: Type casting, null handling, deduplication, business rules
- **Write Pattern**: Incremental merge (upsert on primary key)
- **Schema**: Standardized, validated
- **Quality Gates**: Must pass Great Expectations validation

**Banking Example**: Transactions with valid amounts, standardized currencies, AML flags applied, duplicates removed.

### 🥇 Gold Layer — "Aggregate and Serve"
- **Purpose**: Business-level views optimized for consumption
- **Transformations**: Joins, aggregations, calculations
- **Write Pattern**: Full refresh (consistent aggregations)
- **Schema**: Denormalized, business-oriented
- **Consumers**: Analysts, reports, dashboards, regulatory submissions

**Banking Example**: Customer 360 view joining customers + accounts + transactions + AML alerts.

## Implementation in This Project

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Tool** | PySpark | dbt (incremental) | dbt (table) |
| **Storage** | Delta Lake on MinIO | Delta Lake on MinIO | Delta Lake on MinIO |
| **Materialization** | Append | Incremental merge | Full refresh |
| **Quality** | Schema validation | Business rules + GE | Aggregation integrity |
| **Schedule** | Hourly | After Bronze | After Silver |
| **Consumers** | Silver layer | Gold layer | End users, CBUAE |

## Benefits Realized

1. **Time Travel**: Delta Lake enables querying any historical version
2. **Rollback**: Bad data can be reverted without re-ingestion
3. **Lineage**: Every record traceable from Gold back to source file
4. **Testing**: Each layer independently testable with dbt and GE
5. **Flexibility**: New Gold views without re-processing source data
