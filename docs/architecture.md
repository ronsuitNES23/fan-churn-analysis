# Architecture Documentation

## System Overview

The Fan Churn Analysis project implements a **Medallion Architecture** (Bronze → Silver → Gold) data lakehouse pattern on Databricks. This architecture provides progressive data refinement from raw ingestion through business-ready analytics.

---

## Architecture Diagram

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌────────────────┐       │
│  │   CRM    │  │ Ticketing│  │ Web Analytics│  │  Finance       │       │
│  │  System  │  │   API    │  │   Platform   │  │  System        │       │
│  └─────┬────┘  └─────┬────┘  └──────┬───────┘  └────────┬───────┘       │
└────────┼─────────────┼──────────────┼──────────────────┼─────────────────┘
         │             │              │                  │
         └─────────────┴──────────────┴──────────────────┘
                              │
                       [Raw Data Files]
                     .xlsx, .csv, .json
                              │
                              ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                          BRONZE LAYER                                      │
│                     (Raw, Unprocessed Data)                               │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  fan_churn_bronze (Delta Table)                                     │ │
│  │  • Append-only writes                                                │ │
│  │  • All data as strings                                               │ │
│  │  • Preserves source format                                           │ │
│  │  • Includes audit columns (ingestion_timestamp, source_file)         │ │
│  │  • Partitioned by: ingestion_date                                    │ │
│  │  • Retention: 90 days                                                │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │
                   [PySpark Transformation]
                  • Deduplication
                  • Type conversion
                  • Validation
                  • Standardization
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                          SILVER LAYER                                      │
│                  (Cleaned, Validated Data)                                │
│                                                                           │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │  fan_churn_silver (Delta Table)                                     │ │
│  │  • Overwrite mode (can be rebuilt from Bronze)                       │ │
│  │  • Proper data types (DATE, INT, DOUBLE, BOOLEAN)                    │ │
│  │  • Deduplicated (1 record per fan_id)                                │ │
│  │  • Schema enforcement                                                 │ │
│  │  • Derived columns (tenure, recency, engagement flags)               │ │
│  │  • Z-Ordered by: ticket_tier, renewal_flag                           │ │
│  │  • Quality: 92%+ completeness                                         │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │
                   [Aggregation & KPI Calc]
                  • Business logic
                  • Metrics computation
                  • Segmentation
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                           GOLD LAYER                                       │
│                   (Business-Ready Analytics)                              │
│                                                                           │
│  ┌──────────────────────┐  ┌──────────────────────┐                     │
│  │ gold_churn_by_tier   │  │ gold_cohort_churn    │                     │
│  │ (Tier-level KPIs)    │  │ (Monthly cohorts)    │                     │
│  └──────────────────────┘  └──────────────────────┘                     │
│                                                                           │
│  ┌──────────────────────┐  ┌──────────────────────┐                     │
│  │ gold_clv_by_tier     │  │ gold_fan_clv         │                     │
│  │ (Tier CLV metrics)   │  │ (Individual CLV)     │                     │
│  └──────────────────────┘  └──────────────────────┘                     │
│                                                                           │
│  ┌──────────────────────┐  ┌──────────────────────┐                     │
│  │ gold_reactivation_roi│  │ gold_engagement_     │                     │
│  │ (Win-back metrics)   │  │ scores (0-100 scale) │                     │
│  └──────────────────────┘  └──────────────────────┘                     │
│                                                                           │
│  ┌──────────────────────┐  ┌──────────────────────┐                     │
│  │ gold_revenue_at_risk │  │ gold_executive_kpis  │                     │
│  │ (Lost revenue calc)  │  │ (Dashboard summary)  │                     │
│  └──────────────────────┘  └──────────────────────┘                     │
└───────────────────────────┬───────────────────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                      CONSUMPTION LAYER                                     │
│                                                                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │   Tableau   │  │  Power BI   │  │   Python    │  │  Databricks │   │
│  │  Dashboard  │  │  Reports    │  │  ML Models  │  │     SQL     │   │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## Layer Specifications

### Bronze Layer

**Purpose**: Land raw data exactly as received from source systems.

**Characteristics**:
- **Write Mode**: Append-only (audit trail)
- **Schema**: All columns as STRING to preserve raw formats
- **Data Quality**: None - accepts all data as-is
- **Partitioning**: By `ingestion_date` for efficient pruning
- **Retention**: 90 days (configurable)
- **Use Cases**:
  - Audit trail and compliance
  - Rebuild Silver layer if issues arise
  - Debug data source problems

**Key Operations**:
```python
df_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("ingestion_date")
    .saveAsTable(bronze_table)
```

---

### Silver Layer

**Purpose**: Clean, validated, conformed data ready for analytics.

**Characteristics**:
- **Write Mode**: Overwrite (can be rebuilt from Bronze)
- **Schema**: Strongly typed (DATE, INT, DOUBLE, BOOLEAN)
- **Data Quality**: 
  - Deduplication (fan_id as natural key)
  - Type validation
  - Business rule enforcement
  - 92%+ completeness target
- **Optimization**: 
  - Z-Ordered by `(ticket_tier, renewal_flag)` for query performance
  - Statistics collected on all columns
- **Use Cases**:
  - Ad-hoc analysis
  - ML feature store
  - Gold layer aggregations

**Key Transformations**:
1. Deduplication (keep latest per fan_id)
2. Type conversion (STRING → proper types)
3. Date parsing (multiple formats → ISO 8601)
4. Email validation (regex check)
5. Tier standardization (variations → BRONZE/SILVER/GOLD/PLATINUM)
6. Derived columns (tenure, recency, engagement flags)

**Quality Checks**:
```python
# Completeness
assert df_silver.filter(col("fan_id").isNull()).count() == 0

# Uniqueness
assert df_silver.count() == df_silver.select("fan_id").distinct().count()

# Date logic
assert df_silver.filter(col("join_date") > col("last_login")).count() == 0
```

---

### Gold Layer

**Purpose**: Business-level aggregations and KPIs for analytics/reporting.

**Characteristics**:
- **Write Mode**: Overwrite (derived from Silver)
- **Schema**: Business-domain specific (wide tables)
- **Data Quality**: Pre-aggregated, validated metrics
- **Optimization**: Small tables, heavily cached
- **Use Cases**:
  - Executive dashboards
  - BI tool consumption
  - Scheduled reports

**Tables**:

| Table Name | Purpose | Grain | Update Frequency |
|------------|---------|-------|------------------|
| `gold_churn_by_tier` | Churn metrics by tier | 1 row per tier | Daily |
| `gold_cohort_churn` | Churn by join month | 1 row per month | Daily |
| `gold_clv_by_tier` | CLV metrics by tier | 1 row per tier | Daily |
| `gold_fan_clv` | Individual fan CLV | 1 row per fan | Daily |
| `gold_reactivation_roi` | Win-back ROI by tier | 1 row per tier | Weekly |
| `gold_engagement_scores` | Engagement scoring | 1 row per fan | Daily |
| `gold_revenue_at_risk` | Revenue loss by tier | 1 row per tier | Daily |
| `gold_executive_kpis` | Dashboard summary | 1 row (snapshot) | Daily |

---

## Data Flow

### Ingestion Pipeline

```
1. Raw File Upload
   ↓
2. Bronze Ingestion (01_bronze_ingestion.py)
   - Read Excel/CSV
   - Add audit columns
   - Write to Delta (append)
   ↓
3. Silver Transformation (02_silver_transformation.py)
   - Read Bronze
   - Deduplicate
   - Clean & validate
   - Type convert
   - Write to Delta (overwrite)
   ↓
4. Gold KPI Generation (03_gold_kpis.py)
   - Read Silver
   - Aggregate metrics
   - Calculate KPIs
   - Write Gold tables (overwrite)
```

### Orchestration Options

**Option 1: Databricks Jobs**
- Schedule: Daily at 2 AM UTC
- Tasks: Sequential (Bronze → Silver → Gold)
- Cluster: Auto-scaling, terminates after completion
- Notifications: Email on failure

**Option 2: Delta Live Tables (DLT)**
- Declarative pipeline definition
- Auto-retry on failure
- Built-in data quality checks
- Continuous or triggered mode

**Option 3: Airflow/Prefect**
- Complex dependencies
- External system integration
- Custom retry logic

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Storage** | Delta Lake | ACID transactions, time travel, schema evolution |
| **Compute** | Apache Spark (PySpark) | Distributed data processing |
| **Platform** | Databricks | Unified analytics, notebook environment |
| **Catalog** | Unity Catalog | Data governance, access control |
| **Orchestration** | Databricks Jobs / DLT | Workflow scheduling |
| **Version Control** | Git / GitHub | Code versioning, collaboration |
| **Data Quality** | Great Expectations | Automated validation, profiling |

---

## Performance Considerations

### Partitioning Strategy

**Bronze Layer**:
```python
.partitionBy("ingestion_date")
```
- Efficient pruning for incremental loads
- Easy to delete old data

**Silver/Gold Layers**:
- No partitioning (small datasets, < 1M rows)
- Use Z-Ordering instead for query optimization

### Z-Ordering

```sql
OPTIMIZE fan_churn_silver ZORDER BY (ticket_tier, renewal_flag);
```

**Why**: 
- Clusters data by commonly filtered columns
- Reduces data scanning by 70-90%
- Improves query latency

### Caching

```python
df_silver.cache()  # Keep in memory for multiple operations
```

**When to use**:
- Data fits in cluster memory
- DataFrame reused multiple times in session
- Interactive analysis in notebooks

---

## Data Quality Framework

### Validation Layers

**Bronze → Silver**:
- ✅ Schema validation
- ✅ Null checks on key columns
- ✅ Duplicate detection
- ✅ Type conversion errors
- ✅ Date format parsing

**Silver → Gold**:
- ✅ Business rule validation
- ✅ Referential integrity
- ✅ Metric bounds checking (e.g., churn rate 0-100%)
- ✅ Logical consistency (join_date < last_login)

### Monitoring

**Metrics Tracked**:
- Record counts (Bronze/Silver/Gold)
- Data quality scores
- Processing duration
- Error rates
- Schema drift

**Alerting**:
- Email on job failure
- Slack notification on quality issues
- Dashboard for real-time monitoring

---

## Security & Governance

### Access Control

```sql
-- Read-only for analysts
GRANT SELECT ON SCHEMA dev_fan_analytics.fan_churn TO analysts;

-- Full access for data engineers
GRANT ALL PRIVILEGES ON SCHEMA dev_fan_analytics.fan_churn TO data_engineers;
```

### Data Lineage

Tracked automatically by Unity Catalog:
- Source system → Bronze table
- Bronze → Silver transformations
- Silver → Gold aggregations
- Downstream BI tool usage

### PII Handling

**Sensitive Fields**:
- `email`: Encrypted in production
- `first_name`, `last_name`: Masked in non-prod environments

**Compliance**:
- GDPR: Right to deletion (Delta Lake `DELETE` support)
- Data retention: 90 days Bronze, 1 year Silver/Gold

---

## Disaster Recovery

### Backup Strategy

**Time Travel**:
```sql
-- Restore to 7 days ago
SELECT * FROM fan_churn_silver VERSION AS OF 7 DAYS AGO;
```

**Delta Lake Versioning**:
- Automatic versioning on every write
- Rollback capability
- Audit history

### Recovery Procedures

**Scenario 1: Bad data loaded**
```sql
-- Restore previous version
RESTORE TABLE fan_churn_silver TO VERSION AS OF 5;
```

**Scenario 2: Silver layer corruption**
```python
# Rebuild from Bronze
spark.sql("DROP TABLE IF EXISTS fan_churn_silver")
# Re-run 02_silver_transformation.py
```

**Scenario 3: Complete failure**
- Re-ingest from source systems
- Replay Bronze → Silver → Gold pipeline

---

## Future Enhancements

1. **Real-Time Streaming**
   - Kafka integration for live data feeds
   - Structured Streaming for Bronze ingestion

2. **Machine Learning**
   - Churn prediction model (Random Forest)
   - Propensity scoring
   - MLflow for model tracking

3. **Advanced Analytics**
   - Customer segmentation (RFM analysis)
   - Cohort retention curves
   - A/B test framework

4. **Scalability**
   - Multi-region deployment
   - Incremental processing (Delta Lake MERGE)
   - Partition pruning optimization

---

**Document Version**: 1.0  
**Last Updated**: 2024  
**Maintained By**: Data Engineering Team
