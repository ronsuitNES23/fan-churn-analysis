# Databricks Setup Guide

## Prerequisites

- Databricks workspace (Community Edition or full workspace)
- GitHub account
- Python 3.9+ (for local development)
- Git installed locally

---

## Step 1: Create Databricks Workspace

### Option A: Community Edition (Free)
1. Go to [databricks.com/try-databricks](https://databricks.com/try-databricks)
2. Sign up for Community Edition
3. Verify your email and log in

### Option B: Cloud Provider (AWS/Azure/GCP)
1. Set up Databricks through your cloud provider
2. Configure workspace access

---

## Step 2: Create a Cluster

1. In Databricks, click **Compute** in the left sidebar
2. Click **Create Cluster**
3. Configure cluster:
   - **Cluster Name**: `fan-churn-cluster`
   - **Cluster Mode**: Standard
   - **Databricks Runtime Version**: 13.3 LTS or later
   - **Node Type**: 
     - Community Edition: Default (no choice)
     - Paid: i3.xlarge or similar (2 workers minimum)
   - **Auto Termination**: 60 minutes
4. Click **Create Cluster**
5. Wait for cluster to start (2-5 minutes)

---

## Step 3: Connect GitHub Repository

1. In Databricks, click **Workspace** → **Repos**
2. Click **Add Repo**
3. Enter:
   - **Git repository URL**: `https://github.com/YOUR_USERNAME/fan-churn-analysis`
   - **Git provider**: GitHub
4. Click **Create Repo**

**Alternative: Clone via CLI**
```bash
# In Databricks notebook
%sh
git clone https://github.com/YOUR_USERNAME/fan-churn-analysis.git
```

---

## Step 4: Upload Data

### Method 1: Databricks UI
1. Click **Data** in sidebar
2. Click **Create Table**
3. Click **Upload File**
4. Upload `fan_churn_analysis_bronze_layer_100k.xlsx`
5. Select destination: `dbfs:/FileStore/fan_churn/raw/`

### Method 2: Via Notebook
```python
# In a Databricks notebook
dbutils.fs.cp(
    "file:/local/path/fan_churn_analysis_bronze_layer_100k.xlsx",
    "dbfs:/FileStore/fan_churn/raw/fan_churn_data.xlsx"
)
```

### Method 3: Convert to CSV First (Recommended for large files)
```python
# Locally, convert Excel to CSV
import pandas as pd
df = pd.read_excel('fan_churn_analysis_bronze_layer_100k.xlsx')
df.to_csv('fan_churn_data.csv', index=False)

# Then upload CSV to Databricks
```

---

## Step 5: Create Catalog and Schema

Run this in a Databricks SQL notebook or cell:

```sql
-- Create catalog (Unity Catalog)
CREATE CATALOG IF NOT EXISTS dev_fan_analytics;

-- Create schema
CREATE SCHEMA IF NOT EXISTS dev_fan_analytics.fan_churn;

-- Verify
SHOW SCHEMAS IN dev_fan_analytics;
```

**Note**: If Unity Catalog is not enabled, use:
```sql
CREATE DATABASE IF NOT EXISTS fan_churn;
USE fan_churn;
```

---

## Step 6: Run the Pipeline

### Option A: Manual Notebook Execution

1. Open `notebooks/01_bronze_ingestion.py`
2. Attach to your cluster
3. Update widget parameters if needed:
   - `catalog`: dev_fan_analytics
   - `schema`: fan_churn
   - `source_path`: dbfs:/FileStore/fan_churn/raw/
4. Run all cells (Ctrl/Cmd + Shift + Enter)
5. Repeat for `02_silver_transformation.py`
6. Repeat for `03_gold_kpis.py`

### Option B: Workflow (Automated)

1. Click **Workflows** in sidebar
2. Click **Create Job**
3. Configure:
   - **Job Name**: Fan Churn ETL Pipeline
   - **Task 1**: Bronze Ingestion
     - **Type**: Notebook
     - **Path**: `/Repos/YOUR_USERNAME/fan-churn-analysis/notebooks/01_bronze_ingestion`
     - **Cluster**: fan-churn-cluster
   - **Task 2**: Silver Transformation
     - **Depends on**: Task 1
     - **Path**: `/Repos/YOUR_USERNAME/fan-churn-analysis/notebooks/02_silver_transformation`
   - **Task 3**: Gold KPIs
     - **Depends on**: Task 2
     - **Path**: `/Repos/YOUR_USERNAME/fan-churn-analysis/notebooks/03_gold_kpis`
4. **Schedule** (optional): Daily at 2 AM UTC
5. Click **Create**
6. Click **Run Now** to test

---

## Step 7: Verify Data

Run verification queries:

```sql
-- Check Bronze layer
SELECT COUNT(*) as bronze_records 
FROM dev_fan_analytics.fan_churn.fan_churn_bronze;

-- Check Silver layer
SELECT COUNT(*) as silver_records 
FROM dev_fan_analytics.fan_churn.fan_churn_silver;

-- Check Gold tables
SHOW TABLES IN dev_fan_analytics.fan_churn 
WHERE tableName LIKE 'gold_%';

-- View executive KPIs
SELECT * FROM dev_fan_analytics.fan_churn.gold_executive_kpis;
```

---

## Step 8: Configure Permissions (Optional)

If working in a team:

```sql
-- Grant read access to analysts
GRANT SELECT ON SCHEMA dev_fan_analytics.fan_churn TO `analysts@yourcompany.com`;

-- Grant write access to data engineers
GRANT ALL PRIVILEGES ON SCHEMA dev_fan_analytics.fan_churn TO `data-engineers@yourcompany.com`;
```

---

## Troubleshooting

### Issue: "Table not found"
**Solution**: Ensure catalog and schema are created:
```python
spark.sql("USE CATALOG dev_fan_analytics")
spark.sql("USE fan_churn")
```

### Issue: "Excel file won't load"
**Solution**: Convert to CSV first:
```python
import pandas as pd
df = pd.read_excel('/dbfs/FileStore/fan_churn/raw/fan_churn_data.xlsx')
df.to_csv('/dbfs/FileStore/fan_churn/raw/fan_churn_data.csv', index=False)
```

### Issue: "Cluster won't start"
**Solution**: 
- Check cluster configuration
- For Community Edition: Only 1 cluster allowed
- Terminate other clusters first

### Issue: "Permission denied"
**Solution**: 
- Check workspace admin has granted you access
- Verify Unity Catalog permissions

### Issue: "Out of memory"
**Solution**:
- Increase cluster size (if paid workspace)
- Process data in batches
- Reduce partition count

---

## Performance Optimization

### 1. Enable Adaptive Query Execution
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 2. Z-Order Tables for Better Query Performance
```sql
OPTIMIZE dev_fan_analytics.fan_churn.fan_churn_silver 
ZORDER BY (ticket_tier, renewal_flag);
```

### 3. Vacuum Old Files (after 7 days)
```sql
VACUUM dev_fan_analytics.fan_churn.fan_churn_bronze RETAIN 168 HOURS;
```

### 4. Collect Table Statistics
```sql
ANALYZE TABLE dev_fan_analytics.fan_churn.fan_churn_silver 
COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Next Steps

1. **Explore Data**: Run `04_exploratory_analysis.ipynb`
2. **Build ML Model**: Run `05_ml_churn_prediction.ipynb`
3. **Connect BI Tool**: 
   - Tableau: Connect via Databricks connector
   - Power BI: Use Databricks ODBC driver
4. **Schedule Job**: Set up daily/weekly refresh
5. **Set Up Alerts**: Monitor KPIs and data quality

---

## Useful Databricks Commands

```python
# List files in DBFS
dbutils.fs.ls("dbfs:/FileStore/fan_churn/")

# Copy files
dbutils.fs.cp("source", "destination")

# Remove files
dbutils.fs.rm("dbfs:/path/to/file", True)

# Display DataFrame
display(df)

# Get current database
spark.sql("SELECT current_database()").show()

# List all tables
spark.sql("SHOW TABLES").show()

# Describe table
spark.sql("DESCRIBE EXTENDED table_name").show(100, False)
```

---

## Additional Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Guide](https://docs.delta.io/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

**Document Version**: 1.0  
**Last Updated**: 2024  
**Support**: Open an issue on GitHub
