# Data Dictionary - Fan Churn Analysis

## Bronze Layer Schema

| Column Name | Data Type | Description | Source | Nullable | Quality Issues |
|-------------|-----------|-------------|--------|----------|----------------|
| `fan_id` | STRING | Unique fan identifier | CRM System | No | Duplicates possible (~2%) |
| `first_name` | STRING | Fan's first name | CRM System | Yes | Whitespace, case inconsistencies |
| `last_name` | STRING | Fan's last name | CRM System | Yes | Whitespace, case inconsistencies |
| `email` | STRING | Contact email address | CRM System | Yes | Invalid formats (~8%), missing @ symbols |
| `join_date` | STRING | Date fan joined | CRM System | Yes | Mixed formats, invalid dates |
| `ticket_tier` | STRING | Membership tier | Ticketing API | Yes | Variations (bronze/BRONZE/B) |
| `games_attended_raw` | STRING | Number of games attended | Ticketing API | Yes | Negative values, null (~18%) |
| `last_login` | STRING | Last platform login date | Web Analytics | Yes | Future dates, format issues |
| `total_spend_raw` | STRING | Lifetime spending (USD) | Finance System | Yes | Negative values, $ symbols |
| `renewal_flag_raw` | STRING | Renewal status | CRM System | Yes | Multiple formats (1/0, Yes/No, TRUE/FALSE) |
| `data_source` | STRING | Source system name | System | Yes | Variations (CRM/crm/CRM_SYSTEM) |
| `ingestion_timestamp` | TIMESTAMP | Data load timestamp | ETL | No | Auto-generated |
| `source_file` | STRING | Source filename | ETL | No | Auto-generated |

---

## Silver Layer Schema

| Column Name | Data Type | Description | Transformation | Nullable |
|-------------|-----------|-------------|----------------|----------|
| `fan_id` | STRING | Unique fan identifier (deduplicated) | Cleaned | No |
| `first_name` | STRING | Fan's first name | Trimmed, proper case | Yes |
| `last_name` | STRING | Fan's last name | Trimmed, proper case | Yes |
| `email` | STRING | Validated email address | Validated, lowercased | Yes |
| `join_date` | DATE | Date fan joined | Parsed to ISO 8601 | Yes |
| `ticket_tier` | STRING | Standardized tier | Mapped to BRONZE/SILVER/GOLD/PLATINUM | Yes |
| `games_attended` | INTEGER | Games attended (validated) | Converted to int, range 0-100 | Yes |
| `last_login` | DATE | Last login date | Parsed to ISO 8601 | Yes |
| `total_spend` | DOUBLE | Total spend in USD | Converted to double, > 0 only | Yes |
| `renewal_flag` | BOOLEAN | Did fan renew? | Converted to true/false | Yes |
| `data_source` | STRING | Source system | Original value preserved | Yes |
| `days_since_join` | INTEGER | Days since joining | DATEDIFF(CURRENT_DATE, join_date) | Yes |
| `years_active` | DECIMAL(10,2) | Years of membership | days_since_join / 365.25 | Yes |
| `days_since_last_login` | INTEGER | Days since last login | DATEDIFF(CURRENT_DATE, last_login) | Yes |
| `is_active_user` | BOOLEAN | Active in last 30 days? | days_since_last_login <= 30 | No |
| `is_ghost_fan` | BOOLEAN | Zero engagement? | games_attended = 0 OR NULL | No |
| `revenue_tier` | STRING | Revenue category | Based on total_spend thresholds | No |
| `silver_processed_timestamp` | TIMESTAMP | Processing timestamp | Auto-generated | No |

---

## Gold Layer Tables

### 1. gold_churn_by_tier

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `ticket_tier` | STRING | Membership tier | From Silver |
| `total_fans` | BIGINT | Total fans in tier | COUNT(*) |
| `renewed` | BIGINT | Fans who renewed | SUM(renewal_flag = true) |
| `churned` | BIGINT | Fans who churned | SUM(renewal_flag = false) |
| `churn_rate_pct` | DECIMAL(5,2) | Churn rate % | (churned / total) × 100 |
| `retention_rate_pct` | DECIMAL(5,2) | Retention rate % | (renewed / total) × 100 |

### 2. gold_cohort_churn

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `cohort_month` | DATE | Month fans joined | DATE_TRUNC('month', join_date) |
| `cohort_size` | BIGINT | Fans in cohort | COUNT(*) |
| `renewed` | BIGINT | Renewed count | SUM(renewal_flag = true) |
| `churned` | BIGINT | Churned count | SUM(renewal_flag = false) |
| `cohort_churn_rate` | DECIMAL(5,2) | Cohort churn % | (churned / cohort_size) × 100 |
| `avg_tenure_years` | DECIMAL(10,2) | Average tenure | AVG(years_active) |

### 3. gold_clv_by_tier

| Column Name | Data Type | Description | Formula |
|-------------|-----------|-------------|---------|
| `ticket_tier` | STRING | Membership tier | From Silver |
| `avg_annual_revenue` | DECIMAL(10,2) | Average annual revenue | AVG(total_spend / years_active) |
| `avg_tenure_years` | DECIMAL(10,2) | Average tenure | AVG(years_active) |
| `retention_rate` | DECIMAL(5,3) | Retention rate | renewed / total |
| `fan_count` | BIGINT | Fans in tier | COUNT(*) |
| `total_revenue` | DECIMAL(15,2) | Total tier revenue | SUM(total_spend) |
| `historical_clv` | DECIMAL(10,2) | Historical CLV | avg_annual_revenue × avg_tenure_years |
| `predictive_clv` | DECIMAL(10,2) | Predictive CLV | avg_annual_revenue × (retention / (1 + 0.10 - retention)) |

**Predictive CLV Formula**:
```
CLV = (Average Annual Revenue × Retention Rate) / (1 + Discount Rate - Retention Rate)

Where:
- Discount Rate = 10% (0.10)
- Retention Rate = % of fans who renewed
```

### 4. gold_fan_clv

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `fan_id` | STRING | Fan identifier | From Silver |
| `first_name` | STRING | First name | From Silver |
| `last_name` | STRING | Last name | From Silver |
| `email` | STRING | Email | From Silver |
| `ticket_tier` | STRING | Tier | From Silver |
| `join_date` | DATE | Join date | From Silver |
| `total_spend` | DECIMAL(10,2) | Total spend | From Silver |
| `games_attended` | INTEGER | Games attended | From Silver |
| `renewal_flag` | BOOLEAN | Renewed? | From Silver |
| `years_active` | DECIMAL(10,2) | Years active | From Silver |
| `historical_value` | DECIMAL(10,2) | Historical value | total_spend |
| `avg_annual_spend` | DECIMAL(10,2) | Annual average | total_spend / years_active |
| `projected_3yr_clv` | DECIMAL(10,2) | 3-year projection | total_spend + (avg_annual × 3) if renewed |
| `fan_segment` | STRING | Risk segment | Based on renewal + spend |

**Fan Segments**:
- `HIGH_VALUE_CHURNED`: Churned, spend > $5,000
- `MEDIUM_VALUE_CHURNED`: Churned, spend > $2,000
- `LOW_VALUE_CHURNED`: Churned, spend < $2,000
- `HIGH_VALUE_RETAINED`: Retained, spend > $5,000
- `RETAINED`: Retained, any spend

### 5. gold_reactivation_roi

| Column Name | Data Type | Description | Formula |
|-------------|-----------|-------------|---------|
| `ticket_tier` | STRING | Tier | From Silver |
| `churned_count` | BIGINT | Churned fans | COUNT(renewal_flag = false) |
| `cost_per_fan` | DECIMAL(10,2) | Campaign cost per fan | By tier: Bronze=$50, Silver=$75, Gold=$100, Platinum=$150 |
| `expected_reactivation_rate` | DECIMAL(5,3) | Expected success rate | By tier: Bronze=12%, Silver=15%, Gold=18%, Platinum=20% |
| `avg_spend_after_reactivation` | DECIMAL(10,2) | Expected spend if reactivated | By tier: Bronze=$600, Silver=$800, Gold=$1,200, Platinum=$2,000 |
| `expected_reactivations` | DECIMAL(10,2) | Expected returns | churned_count × reactivation_rate |
| `total_campaign_cost` | DECIMAL(15,2) | Total cost | churned_count × cost_per_fan |
| `expected_revenue` | DECIMAL(15,2) | Expected revenue | expected_reactivations × avg_spend |
| `net_profit` | DECIMAL(15,2) | Net profit | expected_revenue - total_campaign_cost |
| `reactivation_roi_pct` | DECIMAL(10,2) | ROI % | (net_profit / total_campaign_cost) × 100 |

**Reactivation ROI Formula**:
```
ROI = ((Expected Revenue - Campaign Cost) / Campaign Cost) × 100

Where:
- Expected Revenue = Reactivated Fans × Avg Spend After Reactivation
- Campaign Cost = Total Churned Fans × Cost Per Contact
```

### 6. gold_engagement_scores

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `fan_id` | STRING | Fan identifier | From Silver |
| `first_name` | STRING | First name | From Silver |
| `last_name` | STRING | Last name | From Silver |
| `ticket_tier` | STRING | Tier | From Silver |
| `renewal_flag` | BOOLEAN | Renewed? | From Silver |
| `games_attended` | INTEGER | Games attended | From Silver |
| `days_since_last_login` | INTEGER | Days since login | From Silver |
| `total_spend` | DECIMAL(10,2) | Total spend | From Silver |
| `attendance_score` | DECIMAL(5,2) | Attendance score (0-100) | MIN((games_attended / 38) × 100, 100) |
| `recency_score` | DECIMAL(5,2) | Recency score (0-100) | 100 if ≤7 days, 75 if ≤30, 50 if ≤90, 25 if ≤180, else 0 |
| `spending_score` | DECIMAL(5,2) | Spending score (0-100) | 100 if ≥$5k, 75 if ≥$2k, 50 if ≥$1k, 25 if ≥$500, else 10 |
| `engagement_score` | DECIMAL(5,2) | Overall score (0-100) | (attendance × 0.4) + (recency × 0.3) + (spending × 0.3) |
| `engagement_category` | STRING | Engagement level | HIGHLY_ENGAGED (≥70), MODERATELY_ENGAGED (≥40), AT_RISK (<40) |

**Engagement Score Formula**:
```
Engagement Score = (Attendance Score × 40%) + (Recency Score × 30%) + (Spending Score × 30%)

Where:
- Attendance Score = (Games Attended / 38 Home Games) × 100, capped at 100
- Recency Score = Based on days since last login (tiered)
- Spending Score = Based on spending tier thresholds
```

### 7. gold_revenue_at_risk

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `ticket_tier` | STRING | Tier | From Silver |
| `churned_fans` | BIGINT | Churned count | COUNT(renewal_flag = false) |
| `historical_revenue_lost` | DECIMAL(15,2) | Historical loss | SUM(total_spend) |
| `avg_value_per_churned_fan` | DECIMAL(10,2) | Avg per fan | AVG(total_spend) |
| `projected_annual_revenue_loss` | DECIMAL(15,2) | Annual loss | SUM(total_spend / years_active) |
| `projected_3yr_revenue_loss` | DECIMAL(15,2) | 3-year loss | projected_annual × 3 |

### 8. gold_executive_kpis

| Column Name | Data Type | Description | Calculation |
|-------------|-----------|-------------|-------------|
| `total_fans` | BIGINT | Total fan count | COUNT(*) |
| `unique_fans` | BIGINT | Unique fans | COUNT(DISTINCT fan_id) |
| `retained_fans` | BIGINT | Renewed count | SUM(renewal_flag = true) |
| `churned_fans` | BIGINT | Churned count | SUM(renewal_flag = false) |
| `churn_rate_pct` | DECIMAL(5,2) | Churn % | (churned / total) × 100 |
| `total_revenue` | DECIMAL(15,2) | Total revenue | SUM(total_spend) |
| `arpu` | DECIMAL(10,2) | Avg Revenue Per User | AVG(total_spend) |
| `revenue_lost_to_churn` | DECIMAL(15,2) | Lost revenue | SUM(total_spend WHERE churned) |
| `avg_games_attended` | DECIMAL(5,1) | Avg attendance | AVG(games_attended) |
| `ghost_fans` | BIGINT | Zero engagement | COUNT(games_attended = 0) |
| `avg_days_since_login` | INTEGER | Avg recency | AVG(days_since_last_login) |
| `platinum_count` | BIGINT | Platinum fans | COUNT(tier = PLATINUM) |
| `gold_count` | BIGINT | Gold fans | COUNT(tier = GOLD) |
| `silver_count` | BIGINT | Silver fans | COUNT(tier = SILVER) |
| `bronze_count` | BIGINT | Bronze fans | COUNT(tier = BRONZE) |
| `report_generated_at` | TIMESTAMP | Report timestamp | CURRENT_TIMESTAMP() |

---

## Data Quality Rules

### Bronze → Silver Validations

1. **Deduplication**: Remove duplicate `fan_id` records (keep most recent)
2. **Email Validation**: Must contain @ symbol and valid TLD
3. **Date Formats**: Standardize to ISO 8601 (YYYY-MM-DD)
4. **Tier Values**: Map to standard: BRONZE, SILVER, GOLD, PLATINUM
5. **Numeric Ranges**: 
   - `games_attended`: 0-100
   - `total_spend`: > 0
6. **Renewal Flag**: Convert to boolean (true/false)
7. **Null Handling**: Preserve nulls for analysis

### Silver → Gold Validations

1. **Churn Rate Bounds**: 0-100%
2. **CLV Positive**: All CLV values > 0
3. **Date Logic**: join_date < last_login
4. **Revenue Consistency**: total_spend = sum of individual transactions
5. **Engagement Score Range**: 0-100

---

## Business Rules

### Tier Classification
- **PLATINUM**: Premium tier, highest value
- **GOLD**: High value tier
- **SILVER**: Standard tier
- **BRONZE**: Entry-level tier

### Churn Definition
A fan is considered **churned** if `renewal_flag = false`

### Active User Definition
A fan is **active** if they logged in within the last 30 days

### Ghost Fan Definition
A fan is a **ghost fan** if they have 0 games attended or null attendance

### Revenue Tiers
- **HIGH**: total_spend ≥ $5,000
- **MEDIUM**: $2,000 ≤ total_spend < $5,000
- **LOW**: $500 ≤ total_spend < $2,000
- **MINIMAL**: total_spend < $500

---

## Data Lineage

```
Source Systems (CRM, Ticketing, Web Analytics)
    ↓
Bronze Layer (Raw, append-only)
    ↓
Silver Layer (Cleaned, validated, deduplicated)
    ↓
Gold Layer (Business KPIs, analytics-ready)
    ↓
BI Tools / ML Models (Tableau, Power BI, Python)
```

---

**Last Updated**: 2024  
**Version**: 1.0  
**Contact**: Data Engineering Team
