# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer: Business KPIs & Analytics
# MAGIC 
# MAGIC **Purpose**: Generate business-level KPIs from Silver data
# MAGIC 
# MAGIC **Input**: Silver Delta table (cleaned data)  
# MAGIC **Output**: Multiple Gold tables for analytics
# MAGIC 
# MAGIC **KPIs Generated**:
# MAGIC - Churn Rate & Retention
# MAGIC - Customer Lifetime Value (CLV)
# MAGIC - Reactivation ROI
# MAGIC - Engagement Scores
# MAGIC - Revenue Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

dbutils.widgets.text("catalog", "dev_fan_analytics", "Catalog Name")
dbutils.widgets.text("schema", "fan_churn", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")

silver_table = f"{catalog}.{schema_name}.fan_churn_silver"
gold_schema = f"{catalog}.{schema_name}"

print(f"Source: {silver_table}")
print(f"Gold Schema: {gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read Silver Data

# COMMAND ----------

df_silver = spark.table(silver_table)
print(f"Silver records: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. CHURN RATE ANALYSIS

# COMMAND ----------

# Overall churn metrics
churn_overall = spark.sql(f"""
SELECT 
    COUNT(*) as total_fans,
    SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) as renewed_fans,
    SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) as churned_fans,
    ROUND(
        SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as churn_rate_pct,
    ROUND(
        SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as retention_rate_pct
FROM {silver_table}
WHERE renewal_flag IS NOT NULL
""")

display(churn_overall)

# COMMAND ----------

# Churn by ticket tier
churn_by_tier = spark.sql(f"""
SELECT 
    ticket_tier,
    COUNT(*) as total_fans,
    SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) as renewed,
    SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) as churned,
    ROUND(
        SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as churn_rate_pct,
    ROUND(
        SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as retention_rate_pct
FROM {silver_table}
WHERE renewal_flag IS NOT NULL
    AND ticket_tier IS NOT NULL
GROUP BY ticket_tier
ORDER BY churn_rate_pct DESC
""")

# Save as Gold table
(churn_by_tier.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_churn_by_tier"))

display(churn_by_tier)

# COMMAND ----------

# Cohort churn analysis (by join month)
cohort_churn = spark.sql(f"""
SELECT 
    DATE_TRUNC('month', join_date) as cohort_month,
    COUNT(*) as cohort_size,
    SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) as renewed,
    SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) as churned,
    ROUND(
        SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
        2
    ) as cohort_churn_rate,
    ROUND(AVG(years_active), 2) as avg_tenure_years
FROM {silver_table}
WHERE renewal_flag IS NOT NULL
    AND join_date IS NOT NULL
GROUP BY DATE_TRUNC('month', join_date)
ORDER BY cohort_month
""")

(cohort_churn.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_cohort_churn"))

display(cohort_churn)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. CUSTOMER LIFETIME VALUE (CLV)

# COMMAND ----------

# Historical CLV by tier
clv_by_tier = spark.sql(f"""
WITH tier_metrics AS (
    SELECT 
        ticket_tier,
        AVG(total_spend / NULLIF(years_active, 0)) as avg_annual_revenue,
        AVG(years_active) as avg_tenure_years,
        SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as retention_rate,
        COUNT(*) as fan_count,
        SUM(total_spend) as total_revenue
    FROM {silver_table}
    WHERE years_active > 0
        AND total_spend > 0
        AND ticket_tier IS NOT NULL
    GROUP BY ticket_tier
)
SELECT 
    ticket_tier,
    ROUND(avg_annual_revenue, 2) as avg_annual_revenue,
    ROUND(avg_tenure_years, 2) as avg_tenure_years,
    ROUND(retention_rate, 3) as retention_rate,
    fan_count,
    ROUND(total_revenue, 2) as total_revenue,
    -- Simple CLV: Avg Annual Revenue × Avg Tenure
    ROUND(avg_annual_revenue * avg_tenure_years, 2) as historical_clv,
    -- Predictive CLV: Annual Revenue × (Retention / (1 + Discount - Retention))
    -- Using 10% discount rate
    ROUND(
        avg_annual_revenue * (retention_rate / (1 + 0.10 - retention_rate)),
        2
    ) as predictive_clv
FROM tier_metrics
ORDER BY predictive_clv DESC
""")

(clv_by_tier.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_clv_by_tier"))

display(clv_by_tier)

# COMMAND ----------

# Individual fan CLV
fan_clv = spark.sql(f"""
SELECT 
    fan_id,
    first_name,
    last_name,
    email,
    ticket_tier,
    join_date,
    total_spend,
    games_attended,
    renewal_flag,
    years_active,
    -- Historical value (total spent to date)
    ROUND(total_spend, 2) as historical_value,
    -- Average annual spend
    ROUND(
        total_spend / NULLIF(years_active, 0),
        2
    ) as avg_annual_spend,
    -- Projected 3-year CLV for active fans
    CASE 
        WHEN renewal_flag = true THEN
            ROUND(
                total_spend + (
                    (total_spend / NULLIF(years_active, 0)) * 3
                ),
                2
            )
        ELSE total_spend
    END as projected_3yr_clv,
    -- Risk category
    CASE 
        WHEN renewal_flag = false AND total_spend > 5000 THEN 'HIGH_VALUE_CHURNED'
        WHEN renewal_flag = false AND total_spend > 2000 THEN 'MEDIUM_VALUE_CHURNED'
        WHEN renewal_flag = false THEN 'LOW_VALUE_CHURNED'
        WHEN renewal_flag = true AND total_spend > 5000 THEN 'HIGH_VALUE_RETAINED'
        WHEN renewal_flag = true THEN 'RETAINED'
        ELSE 'UNKNOWN'
    END as fan_segment
FROM {silver_table}
WHERE total_spend IS NOT NULL
    AND years_active > 0
ORDER BY projected_3yr_clv DESC
""")

(fan_clv.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_fan_clv"))

# Show top 20
display(fan_clv.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. REACTIVATION ROI ANALYSIS

# COMMAND ----------

# Reactivation ROI by tier
reactivation_roi = spark.sql(f"""
WITH churned_fans AS (
    SELECT 
        ticket_tier,
        COUNT(*) as churned_count,
        AVG(total_spend) as avg_previous_spend
    FROM {silver_table}
    WHERE renewal_flag = false
        AND ticket_tier IS NOT NULL
    GROUP BY ticket_tier
),
campaign_costs AS (
    -- Campaign cost assumptions by tier
    SELECT 'BRONZE' as tier, 50 as cost_per_fan, 0.12 as expected_reactivation_rate, 600 as avg_spend_after_reactivation UNION ALL
    SELECT 'SILVER', 75, 0.15, 800 UNION ALL
    SELECT 'GOLD', 100, 0.18, 1200 UNION ALL
    SELECT 'PLATINUM', 150, 0.20, 2000
)
SELECT 
    c.tier as ticket_tier,
    cf.churned_count,
    cc.cost_per_fan,
    cc.expected_reactivation_rate,
    cc.avg_spend_after_reactivation,
    -- Expected reactivations
    ROUND(cf.churned_count * cc.expected_reactivation_rate, 0) as expected_reactivations,
    -- Campaign cost
    ROUND(cf.churned_count * cc.cost_per_fan, 2) as total_campaign_cost,
    -- Expected revenue
    ROUND(
        cf.churned_count * cc.expected_reactivation_rate * cc.avg_spend_after_reactivation,
        2
    ) as expected_revenue,
    -- Net profit
    ROUND(
        (cf.churned_count * cc.expected_reactivation_rate * cc.avg_spend_after_reactivation) - 
        (cf.churned_count * cc.cost_per_fan),
        2
    ) as net_profit,
    -- ROI %
    ROUND(
        (((cf.churned_count * cc.expected_reactivation_rate * cc.avg_spend_after_reactivation) - 
          (cf.churned_count * cc.cost_per_fan)) * 100.0 / 
         (cf.churned_count * cc.cost_per_fan)),
        2
    ) as reactivation_roi_pct
FROM churned_fans cf
JOIN campaign_costs cc ON cf.ticket_tier = cc.tier
ORDER BY net_profit DESC
""")

(reactivation_roi.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_reactivation_roi"))

display(reactivation_roi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. ENGAGEMENT SCORE

# COMMAND ----------

# Calculate comprehensive engagement score
engagement_scores = spark.sql(f"""
SELECT 
    fan_id,
    first_name,
    last_name,
    ticket_tier,
    renewal_flag,
    games_attended,
    days_since_last_login,
    total_spend,
    -- Attendance score (0-100)
    CASE 
        WHEN games_attended IS NULL THEN 0
        ELSE LEAST(games_attended * 100.0 / 38, 100)  -- 38 home games per season
    END as attendance_score,
    -- Recency score (0-100)
    CASE 
        WHEN days_since_last_login IS NULL THEN 0
        WHEN days_since_last_login <= 7 THEN 100
        WHEN days_since_last_login <= 30 THEN 75
        WHEN days_since_last_login <= 90 THEN 50
        WHEN days_since_last_login <= 180 THEN 25
        ELSE 0
    END as recency_score,
    -- Spending score (0-100) - percentile-based
    CASE 
        WHEN total_spend IS NULL THEN 0
        WHEN total_spend >= 5000 THEN 100
        WHEN total_spend >= 2000 THEN 75
        WHEN total_spend >= 1000 THEN 50
        WHEN total_spend >= 500 THEN 25
        ELSE 10
    END as spending_score,
    -- Overall engagement score (weighted average: 40% attendance, 30% recency, 30% spend)
    ROUND(
        (
            CASE WHEN games_attended IS NULL THEN 0 ELSE LEAST(games_attended * 100.0 / 38, 100) END * 0.4 +
            CASE 
                WHEN days_since_last_login IS NULL THEN 0
                WHEN days_since_last_login <= 7 THEN 100
                WHEN days_since_last_login <= 30 THEN 75
                WHEN days_since_last_login <= 90 THEN 50
                WHEN days_since_last_login <= 180 THEN 25
                ELSE 0
            END * 0.3 +
            CASE 
                WHEN total_spend IS NULL THEN 0
                WHEN total_spend >= 5000 THEN 100
                WHEN total_spend >= 2000 THEN 75
                WHEN total_spend >= 1000 THEN 50
                WHEN total_spend >= 500 THEN 25
                ELSE 10
            END * 0.3
        ),
        0
    ) as engagement_score,
    -- Risk category
    CASE 
        WHEN ROUND(
            (
                CASE WHEN games_attended IS NULL THEN 0 ELSE LEAST(games_attended * 100.0 / 38, 100) END * 0.4 +
                CASE 
                    WHEN days_since_last_login IS NULL THEN 0
                    WHEN days_since_last_login <= 7 THEN 100
                    WHEN days_since_last_login <= 30 THEN 75
                    WHEN days_since_last_login <= 90 THEN 50
                    WHEN days_since_last_login <= 180 THEN 25
                    ELSE 0
                END * 0.3 +
                CASE 
                    WHEN total_spend IS NULL THEN 0
                    WHEN total_spend >= 5000 THEN 100
                    WHEN total_spend >= 2000 THEN 75
                    WHEN total_spend >= 1000 THEN 50
                    WHEN total_spend >= 500 THEN 25
                    ELSE 10
                END * 0.3
            ),
            0
        ) >= 70 THEN 'HIGHLY_ENGAGED'
        WHEN ROUND(
            (
                CASE WHEN games_attended IS NULL THEN 0 ELSE LEAST(games_attended * 100.0 / 38, 100) END * 0.4 +
                CASE 
                    WHEN days_since_last_login IS NULL THEN 0
                    WHEN days_since_last_login <= 7 THEN 100
                    WHEN days_since_last_login <= 30 THEN 75
                    WHEN days_since_last_login <= 90 THEN 50
                    WHEN days_since_last_login <= 180 THEN 25
                    ELSE 0
                END * 0.3 +
                CASE 
                    WHEN total_spend IS NULL THEN 0
                    WHEN total_spend >= 5000 THEN 100
                    WHEN total_spend >= 2000 THEN 75
                    WHEN total_spend >= 1000 THEN 50
                    WHEN total_spend >= 500 THEN 25
                    ELSE 10
                END * 0.3
            ),
            0
        ) >= 40 THEN 'MODERATELY_ENGAGED'
        ELSE 'AT_RISK'
    END as engagement_category
FROM {silver_table}
""")

(engagement_scores.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_engagement_scores"))

display(engagement_scores.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. REVENUE AT RISK

# COMMAND ----------

# Calculate revenue at risk from churned fans
revenue_at_risk = spark.sql(f"""
SELECT 
    ticket_tier,
    COUNT(*) as churned_fans,
    SUM(total_spend) as historical_revenue_lost,
    AVG(total_spend) as avg_value_per_churned_fan,
    -- Projected annual revenue loss (based on avg annual spend)
    SUM(total_spend / NULLIF(years_active, 0)) as projected_annual_revenue_loss,
    -- 3-year projected loss
    SUM(total_spend / NULLIF(years_active, 0)) * 3 as projected_3yr_revenue_loss
FROM {silver_table}
WHERE renewal_flag = false
    AND total_spend > 0
    AND years_active > 0
    AND ticket_tier IS NOT NULL
GROUP BY ticket_tier
ORDER BY projected_3yr_revenue_loss DESC
""")

(revenue_at_risk.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_revenue_at_risk"))

display(revenue_at_risk)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. EXECUTIVE DASHBOARD KPIs

# COMMAND ----------

# Create executive summary table
executive_kpis = spark.sql(f"""
SELECT 
    -- Fan metrics
    COUNT(*) as total_fans,
    COUNT(DISTINCT fan_id) as unique_fans,
    SUM(CASE WHEN renewal_flag = true THEN 1 ELSE 0 END) as retained_fans,
    SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) as churned_fans,
    ROUND(SUM(CASE WHEN renewal_flag = false THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate_pct,
    
    -- Revenue metrics
    ROUND(SUM(total_spend), 2) as total_revenue,
    ROUND(AVG(total_spend), 2) as arpu,  -- Average Revenue Per User
    ROUND(SUM(CASE WHEN renewal_flag = false THEN total_spend ELSE 0 END), 2) as revenue_lost_to_churn,
    
    -- Engagement metrics
    ROUND(AVG(games_attended), 1) as avg_games_attended,
    SUM(CASE WHEN games_attended = 0 OR games_attended IS NULL THEN 1 ELSE 0 END) as ghost_fans,
    ROUND(AVG(days_since_last_login), 0) as avg_days_since_login,
    
    -- Tier distribution
    SUM(CASE WHEN ticket_tier = 'PLATINUM' THEN 1 ELSE 0 END) as platinum_count,
    SUM(CASE WHEN ticket_tier = 'GOLD' THEN 1 ELSE 0 END) as gold_count,
    SUM(CASE WHEN ticket_tier = 'SILVER' THEN 1 ELSE 0 END) as silver_count,
    SUM(CASE WHEN ticket_tier = 'BRONZE' THEN 1 ELSE 0 END) as bronze_count,
    
    -- Current timestamp
    CURRENT_TIMESTAMP() as report_generated_at
FROM {silver_table}
""")

(executive_kpis.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable(f"{gold_schema}.gold_executive_kpis"))

display(executive_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary

# COMMAND ----------

# List all Gold tables created
gold_tables = spark.sql(f"SHOW TABLES IN {gold_schema}").filter("tableName LIKE 'gold_%'")

print("✅ Gold Layer KPIs Generated\n")
print("Tables created:")
display(gold_tables)

print(f"""
Gold Tables:
- {gold_schema}.gold_churn_by_tier
- {gold_schema}.gold_cohort_churn
- {gold_schema}.gold_clv_by_tier
- {gold_schema}.gold_fan_clv
- {gold_schema}.gold_reactivation_roi
- {gold_schema}.gold_engagement_scores
- {gold_schema}.gold_revenue_at_risk
- {gold_schema}.gold_executive_kpis

Status: Ready for BI tools / ML models
Next Steps: 
  - Connect Tableau/Power BI
  - Build ML churn prediction model
  - Create automated reports
""")
