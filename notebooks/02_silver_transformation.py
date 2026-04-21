# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer: Data Cleaning & Standardization
# MAGIC 
# MAGIC **Purpose**: Transform Bronze data into clean, validated Silver layer
# MAGIC 
# MAGIC **Input**: Bronze Delta table (raw data)  
# MAGIC **Output**: Silver Delta table (cleaned, validated, deduplicated)
# MAGIC 
# MAGIC **Transformations**:
# MAGIC - Deduplication
# MAGIC - Data type conversions
# MAGIC - Date standardization
# MAGIC - Email validation
# MAGIC - Null handling
# MAGIC - Business rule validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import re

# Widgets
dbutils.widgets.text("catalog", "dev_fan_analytics", "Catalog Name")
dbutils.widgets.text("schema", "fan_churn", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema_name}.fan_churn_bronze"
silver_table = f"{catalog}.{schema_name}.fan_churn_silver"

print(f"Source: {bronze_table}")
print(f"Target: {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Bronze Data

# COMMAND ----------

df_bronze = spark.table(bronze_table)

print(f"Bronze records: {df_bronze.count():,}")

# Get latest ingestion only (if multiple loads)
df_latest = (df_bronze
             .withColumn("rank", row_number().over(
                 Window.partitionBy("fan_id")
                 .orderBy(desc("ingestion_timestamp"))))
             .filter(col("rank") == 1)
             .drop("rank"))

print(f"Latest records: {df_latest.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deduplication

# COMMAND ----------

# Remove duplicates based on fan_id, keeping most recent record
df_deduped = (df_latest
              .dropDuplicates(["fan_id"]))

duplicate_count = df_latest.count() - df_deduped.count()
print(f"Duplicates removed: {duplicate_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Type Conversions & Cleaning

# COMMAND ----------

# UDF for email validation
@udf(returnType=BooleanType())
def is_valid_email(email):
    if email is None:
        return False
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

# Clean and standardize
df_cleaned = (df_deduped
    
    # Clean names (trim whitespace, proper case)
    .withColumn("first_name", 
                initcap(trim(col("first_name"))))
    .withColumn("last_name", 
                initcap(trim(col("last_name"))))
    
    # Clean and validate email
    .withColumn("email_clean", 
                lower(trim(regexp_replace(col("email"), r'\s+', ''))))
    .withColumn("email_valid", 
                is_valid_email(col("email_clean")))
    .withColumn("email", 
                when(col("email_valid"), col("email_clean"))
                .otherwise(None))
    .drop("email_clean", "email_valid")
    
    # Standardize ticket tier
    .withColumn("ticket_tier", 
                upper(trim(col("ticket_tier"))))
    .withColumn("ticket_tier",
                when(col("ticket_tier").isin(['BRONZE', 'B', 'BRNZE']), 'BRONZE')
                .when(col("ticket_tier").isin(['SILVER', 'S', 'SLVER']), 'SILVER')
                .when(col("ticket_tier").isin(['GOLD', 'G', 'GLD']), 'GOLD')
                .when(col("ticket_tier").isin(['PLATINUM', 'P', 'PLAT', 'PLATNUM']), 'PLATINUM')
                .otherwise(None))
    
    # Convert games attended to integer
    .withColumn("games_attended",
                when(col("games_attended_raw").cast("int").isNotNull() &
                     (col("games_attended_raw").cast("int") >= 0) &
                     (col("games_attended_raw").cast("int") <= 100),
                     col("games_attended_raw").cast("int"))
                .otherwise(None))
    
    # Convert total spend to double
    .withColumn("total_spend_clean",
                regexp_replace(col("total_spend_raw"), r'[^0-9.]', ''))
    .withColumn("total_spend",
                when(col("total_spend_clean").cast("double") > 0,
                     col("total_spend_clean").cast("double"))
                .otherwise(None))
    .drop("total_spend_clean")
    
    # Convert renewal flag to boolean
    .withColumn("renewal_flag",
                when(col("renewal_flag_raw").cast("int") == 1, True)
                .when(col("renewal_flag_raw").isin(['Yes', 'yes', 'Y', 'TRUE', 'true'], True)
                .when(col("renewal_flag_raw").cast("int") == 0, False)
                .when(col("renewal_flag_raw").isin(['No', 'no', 'N', 'FALSE', 'false']), False)
                .otherwise(None))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Date Standardization

# COMMAND ----------

# UDF to parse various date formats
@udf(returnType=DateType())
def parse_date(date_string):
    if date_string is None or date_string in ['NULL', '0000-00-00', 'TBD', 'Pending']:
        return None
    
    from datetime import datetime
    
    # Try multiple formats
    formats = [
        '%Y-%m-%d',      # ISO format
        '%m/%d/%Y',      # US format
        '%d-%m-%Y',      # European format
        '%Y%m%d',        # Compact format
        '%b %d, %Y',     # Text month
        '%d/%m/%y',      # Short year
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_string).strip(), fmt).date()
        except:
            continue
    
    return None

# Parse dates
df_with_dates = (df_cleaned
    .withColumn("join_date", parse_date(col("join_date")))
    .withColumn("last_login", parse_date(col("last_login")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Derived Columns

# COMMAND ----------

from pyspark.sql.functions import datediff, current_date

df_silver = (df_with_dates
    
    # Calculate tenure
    .withColumn("days_since_join",
                when(col("join_date").isNotNull(),
                     datediff(current_date(), col("join_date")))
                .otherwise(None))
    
    .withColumn("years_active",
                when(col("days_since_join").isNotNull(),
                     round(col("days_since_join") / 365.25, 2))
                .otherwise(None))
    
    # Calculate days since last login
    .withColumn("days_since_last_login",
                when(col("last_login").isNotNull(),
                     datediff(current_date(), col("last_login")))
                .otherwise(None))
    
    # Engagement flags
    .withColumn("is_active_user",
                when(col("days_since_last_login") <= 30, True)
                .otherwise(False))
    
    .withColumn("is_ghost_fan",
                when((col("games_attended").isNull()) | (col("games_attended") == 0), True)
                .otherwise(False))
    
    # Revenue tier
    .withColumn("revenue_tier",
                when(col("total_spend") >= 5000, 'HIGH')
                .when(col("total_spend") >= 2000, 'MEDIUM')
                .when(col("total_spend") >= 500, 'LOW')
                .otherwise('MINIMAL'))
    
    # Add Silver metadata
    .withColumn("silver_processed_timestamp", current_timestamp())
    .withColumn("silver_layer_version", lit("v1.0"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Quality Validation

# COMMAND ----------

# Validation checks
print("=== SILVER LAYER DATA QUALITY ===\n")

total_records = df_silver.count()
print(f"Total Records: {total_records:,}\n")

# Completeness checks
completeness_checks = {
    "fan_id": df_silver.filter(col("fan_id").isNotNull()).count() / total_records,
    "email": df_silver.filter(col("email").isNotNull()).count() / total_records,
    "ticket_tier": df_silver.filter(col("ticket_tier").isNotNull()).count() / total_records,
    "join_date": df_silver.filter(col("join_date").isNotNull()).count() / total_records,
    "renewal_flag": df_silver.filter(col("renewal_flag").isNotNull()).count() / total_records,
}

print("Completeness:")
for field, pct in completeness_checks.items():
    status = "✓" if pct >= 0.85 else "⚠"
    print(f"  {status} {field}: {pct*100:.1f}%")

# Validity checks
print("\nValidity Checks:")

# Check for future dates
future_dates = df_silver.filter(col("join_date") > current_date()).count()
print(f"  {'✓' if future_dates == 0 else '⚠'} Future join dates: {future_dates}")

# Check for negative values
negative_spend = df_silver.filter(col("total_spend") < 0).count()
print(f"  {'✓' if negative_spend == 0 else '⚠'} Negative spend: {negative_spend}")

# Check tier values
valid_tiers = df_silver.filter(
    col("ticket_tier").isin(['BRONZE', 'SILVER', 'GOLD', 'PLATINUM'])
).count()
print(f"  {'✓' if valid_tiers == total_records else '⚠'} Valid tiers: {valid_tiers}/{total_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write to Silver Table

# COMMAND ----------

# Select final columns
silver_columns = [
    "fan_id",
    "first_name",
    "last_name",
    "email",
    "join_date",
    "ticket_tier",
    "games_attended",
    "last_login",
    "total_spend",
    "renewal_flag",
    "data_source",
    "days_since_join",
    "years_active",
    "days_since_last_login",
    "is_active_user",
    "is_ghost_fan",
    "revenue_tier",
    "silver_processed_timestamp",
    "silver_layer_version"
]

df_final = df_silver.select(silver_columns)

# Write as Delta table (overwrite mode for Silver)
(df_final.write
 .format("delta")
 .mode("overwrite")  # Silver can be rebuilt from Bronze
 .option("overwriteSchema", "true")
 .saveAsTable(silver_table))

print(f"✓ Data written to {silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Optimize & Collect Statistics

# COMMAND ----------

# Optimize
spark.sql(f"OPTIMIZE {silver_table} ZORDER BY (ticket_tier, renewal_flag)")

# Collect stats
spark.sql(f"ANALYZE TABLE {silver_table} COMPUTE STATISTICS FOR ALL COLUMNS")

print("✓ Table optimized and statistics collected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary Report

# COMMAND ----------

# Generate summary
summary_df = spark.sql(f"""
SELECT 
    COUNT(*) as total_fans,
    COUNT(DISTINCT fan_id) as unique_fans,
    SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) as renewed,
    SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) as churned,
    ROUND(AVG(total_spend), 2) as avg_spend,
    ROUND(AVG(games_attended), 1) as avg_games_attended,
    COUNT(CASE WHEN ticket_tier = 'PLATINUM' THEN 1 END) as platinum_fans,
    COUNT(CASE WHEN ticket_tier = 'GOLD' THEN 1 END) as gold_fans,
    COUNT(CASE WHEN ticket_tier = 'SILVER' THEN 1 END) as silver_fans,
    COUNT(CASE WHEN ticket_tier = 'BRONZE' THEN 1 END) as bronze_fans
FROM {silver_table}
""")

display(summary_df)

print(f"""
✅ Silver Layer Transformation Complete

Table: {silver_table}
Records: {df_final.count():,}
Quality: High (validated and cleaned)
Next Step: Run notebook 03_gold_kpis.py
""")
