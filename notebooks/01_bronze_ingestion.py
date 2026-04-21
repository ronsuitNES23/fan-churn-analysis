# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Fan Churn Data Ingestion
# MAGIC 
# MAGIC **Purpose**: Ingest raw fan data from Excel/CSV into Bronze Delta table
# MAGIC 
# MAGIC **Input**: Raw Excel file from CRM system  
# MAGIC **Output**: Bronze Delta table with raw, unprocessed data
# MAGIC 
# MAGIC **Layer Characteristics**:
# MAGIC - Append-only (historical record of all data received)
# MAGIC - No transformations or cleaning
# MAGIC - Preserves data lineage
# MAGIC - Source system schema maintained

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime

# Widgets for parameterization (useful for job scheduling)
dbutils.widgets.text("catalog", "dev_fan_analytics", "Catalog Name")
dbutils.widgets.text("schema", "fan_churn", "Schema Name")
dbutils.widgets.text("source_path", "dbfs:/FileStore/fan_churn/raw/", "Source Data Path")

# Get parameters
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
source_path = dbutils.widgets.get("source_path")

# Table names
bronze_table = f"{catalog}.{schema_name}.fan_churn_bronze"

print(f"Bronze Table: {bronze_table}")
print(f"Source Path: {source_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Catalog and Schema

# COMMAND ----------

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")

print(f"✓ Catalog and schema ready: {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Bronze Schema
# MAGIC 
# MAGIC Accept all columns as strings to preserve raw data integrity

# COMMAND ----------

bronze_schema = StructType([
    StructField("fan_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("join_date", StringType(), True),  # String to preserve format inconsistencies
    StructField("ticket_tier", StringType(), True),
    StructField("games_attended_raw", StringType(), True),  # String for invalid values
    StructField("last_login", StringType(), True),
    StructField("total_spend_raw", StringType(), True),
    StructField("renewal_flag_raw", StringType(), True),
    StructField("data_source", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read Source Data

# COMMAND ----------

# Read Excel file
# Note: For production, consider converting Excel to CSV first for better performance
try:
    # Option 1: If already converted to CSV
    df_raw = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "false")  # Use our defined schema
              .schema(bronze_schema)
              .load(f"{source_path}*.csv"))
    
    print(f"✓ Loaded {df_raw.count():,} records from CSV")
    
except Exception as e:
    print(f"CSV load failed: {e}")
    print("Attempting Excel load with pandas...")
    
    # Option 2: Load Excel via pandas (for smaller files)
    import pandas as pd
    
    # List all files in source path
    files = dbutils.fs.ls(source_path)
    excel_files = [f.path for f in files if f.path.endswith('.xlsx')]
    
    if excel_files:
        # Convert DBFS path to local path for pandas
        local_path = excel_files[0].replace("dbfs:/", "/dbfs/")
        
        # Read with pandas
        pdf = pd.read_excel(local_path)
        
        # Convert to Spark DataFrame
        df_raw = spark.createDataFrame(pdf)
        
        print(f"✓ Loaded {df_raw.count():,} records from Excel")
    else:
        raise Exception("No source files found!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Add Metadata Columns

# COMMAND ----------

# Add audit columns
df_bronze = (df_raw
             .withColumn("ingestion_timestamp", current_timestamp())
             .withColumn("source_file", lit(source_path))
             .withColumn("bronze_layer_version", lit("v1.0")))

# Display sample
display(df_bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality Profiling (Pre-Load)

# COMMAND ----------

# Basic quality checks
print("=== BRONZE LAYER DATA PROFILE ===\n")

total_records = df_bronze.count()
print(f"Total Records: {total_records:,}")

# Check for nulls in key columns
for col_name in ["fan_id", "email", "ticket_tier", "renewal_flag_raw"]:
    null_count = df_bronze.filter(col(col_name).isNull()).count()
    null_pct = (null_count / total_records) * 100
    print(f"{col_name}: {null_count:,} nulls ({null_pct:.1f}%)")

# Check for duplicates
duplicate_count = df_bronze.count() - df_bronze.select("fan_id").distinct().count()
print(f"\nDuplicate fan_ids: {duplicate_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Table

# COMMAND ----------

# Write as Delta table with append mode (Bronze is append-only)
(df_bronze.write
 .format("delta")
 .mode("append")  # Bronze layer is append-only for audit trail
 .option("mergeSchema", "true")  # Allow schema evolution
 .saveAsTable(bronze_table))

print(f"✓ Data written to {bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Bronze Table

# COMMAND ----------

# Read back and verify
df_verify = spark.table(bronze_table)

print(f"Bronze table record count: {df_verify.count():,}")
print(f"\nSchema:")
df_verify.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Table Properties and Optimization

# COMMAND ----------

# Optimize table for better query performance
spark.sql(f"OPTIMIZE {bronze_table}")

# Collect statistics
spark.sql(f"ANALYZE TABLE {bronze_table} COMPUTE STATISTICS FOR ALL COLUMNS")

print(f"✓ Table optimized and statistics collected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary

# COMMAND ----------

# Final summary
summary_df = spark.sql(f"""
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT fan_id) as unique_fans,
    MIN(ingestion_timestamp) as first_ingestion,
    MAX(ingestion_timestamp) as latest_ingestion
FROM {bronze_table}
""")

display(summary_df)

print(f"""
✅ Bronze Layer Ingestion Complete

Table: {bronze_table}
Records: {df_verify.count():,}
Status: Ready for Silver transformation
Next Step: Run notebook 02_silver_transformation.py
""")
