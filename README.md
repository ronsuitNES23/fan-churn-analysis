# ⚽ Fan Churn Analysis - Medallion Architecture

A production-grade sports analytics project demonstrating ETL pipeline development using the **Medallion Architecture** (Bronze → Silver → Gold) on Databricks with PySpark.

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)

## 📊 Project Overview

This project analyzes fan engagement, churn patterns, and customer lifetime value (CLV) for a European football club using a scalable data lakehouse architecture. The pipeline processes 100,000+ fan records through three transformation layers to deliver actionable insights for fan retention strategies.

### Business Problem
- **Churn Rate**: ~62% of fans don't renew season tickets
- **Revenue at Risk**: $180M+ from churned premium tier fans
- **Challenge**: Identify at-risk fans and optimize reactivation campaigns

### Solution
Medallion architecture pipeline that:
1. Ingests raw CRM data with quality issues (Bronze)
2. Cleanses and standardizes data (Silver)
3. Generates business KPIs and ML features (Gold)

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                             │
│                    (Raw, As-Is from Source)                      │
│  • Duplicate records • Missing values • Format inconsistencies   │
│  • Type mismatches   • Invalid dates  • Null handling            │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                             │
│                  (Cleaned & Standardized)                        │
│  • Deduplication     • Data validation  • Type standardization   │
│  • Schema enforcement• Null handling    • Business rules         │
└──────────────────────┬──────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                              │
│                    (Business-Level KPIs)                         │
│  • Churn Analytics   • CLV Calculation  • Engagement Scoring     │
│  • Cohort Analysis   • Revenue Metrics  • ML Feature Engineering │
└─────────────────────────────────────────────────────────────────┘
```

## 📁 Repository Structure

```
fan-churn-analysis/
├── README.md                          # This file
├── .gitignore                         # Git ignore patterns
├── requirements.txt                   # Python dependencies
├── databricks.yml                     # Databricks Asset Bundle config
│
├── data/
│   ├── bronze/                        # Raw data files (not in git - too large)
│   │   └── .gitkeep
│   └── sample/                        # Sample data for testing
│       └── fan_churn_sample.csv
│
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Load raw data to Bronze
│   ├── 02_silver_transformation.py    # Bronze → Silver cleaning
│   ├── 03_gold_kpis.py               # Silver → Gold analytics
│   ├── 04_exploratory_analysis.ipynb  # EDA and visualizations
│   └── 05_ml_churn_prediction.ipynb   # ML model development
│
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py                # Configuration constants
│   ├── transformations/
│   │   ├── __init__.py
│   │   ├── bronze_layer.py            # Bronze transformation logic
│   │   ├── silver_layer.py            # Silver transformation logic
│   │   └── gold_layer.py              # Gold KPI calculations
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── data_quality.py            # DQ checks and validation
│   │   ├── spark_helpers.py           # Spark utility functions
│   │   └── logger.py                  # Logging configuration
│   └── schemas/
│       ├── __init__.py
│       ├── bronze_schema.py           # Bronze layer schema
│       ├── silver_schema.py           # Silver layer schema
│       └── gold_schema.py             # Gold layer schema
│
├── sql/
│   ├── bronze/
│   │   └── create_bronze_table.sql
│   ├── silver/
│   │   ├── create_silver_table.sql
│   │   └── validation_queries.sql
│   └── gold/
│       ├── create_gold_tables.sql
│       ├── churn_kpis.sql
│       └── clv_analysis.sql
│
├── tests/
│   ├── __init__.py
│   ├── test_bronze_layer.py
│   ├── test_silver_layer.py
│   ├── test_gold_layer.py
│   └── test_data_quality.py
│
├── pipelines/
│   ├── medallion_pipeline.py          # End-to-end pipeline orchestration
│   └── dlt_pipeline.py                # Delta Live Tables pipeline
│
├── workflows/
│   └── fan_churn_etl.yml              # Databricks job workflow definition
│
└── docs/
    ├── architecture.md                # Detailed architecture guide
    ├── data_dictionary.md             # Column definitions
    ├── kpi_definitions.md             # KPI formulas and business logic
    └── setup_guide.md                 # Databricks setup instructions
```

## 🚀 Quick Start

### Prerequisites
- Databricks workspace (Community Edition or paid)
- Python 3.9+
- Git

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/fan-churn-analysis.git
   cd fan-churn-analysis
   ```

2. **Install dependencies** (for local development)
   ```bash
   pip install -r requirements.txt
   ```

3. **Connect to Databricks**
   - Import this repository as a Databricks Repo
   - Navigate to Workspace → Repos → Add Repo
   - Enter repository URL
   - Attach to your cluster

4. **Upload data**
   ```python
   # Run in Databricks notebook
   dbutils.fs.cp("file:/path/to/fan_churn_bronze_layer_100k.xlsx", 
                 "dbfs:/FileStore/fan_churn/bronze/")
   ```

5. **Run the pipeline**
   - Open `notebooks/01_bronze_ingestion.py`
   - Attach to cluster and run all cells
   - Proceed through notebooks 02 → 03 sequentially

## 📊 Key Performance Indicators

### Churn Metrics
- **Overall Churn Rate**: 61.9%
- **Premium Tier Churn**: 58.2% (Platinum/Gold combined)
- **High-Risk Fans**: 15,200+ fans with low engagement scores

### Revenue Impact
- **Total Revenue at Risk**: $184.3M
- **Average CLV (Platinum)**: $12,400
- **Lost Annual Revenue**: $68.2M from churned fans

### Engagement Patterns
- **Avg Games Attended**: 19.6 / 38 home games (51.6%)
- **Ghost Fans** (0 games): 7.9%
- **VIP Fans** (30+ games): 12.3%

## 🔧 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Databricks | Unified analytics platform |
| **Processing** | PySpark | Distributed data processing |
| **Storage** | Delta Lake | ACID transactions, time travel |
| **Orchestration** | Databricks Jobs | Workflow scheduling |
| **Version Control** | Git | Code versioning |
| **Data Quality** | Great Expectations | Automated validation |

## 📈 Pipeline Performance

- **Data Volume**: 100,000 fan records
- **Processing Time**: 
  - Bronze ingestion: ~45 seconds
  - Silver transformation: ~2 minutes
  - Gold aggregation: ~1 minute
- **Data Quality**: 92.4% completeness after Silver layer
- **Deduplication**: Removed 1,984 duplicate records

## 🧪 Data Quality Checks

**Bronze → Silver Validations:**
- ✅ Remove duplicates (based on fan_id)
- ✅ Standardize date formats (ISO 8601)
- ✅ Validate email formats
- ✅ Clean ticket tier values
- ✅ Handle missing values (configurable strategy)
- ✅ Type conversions (string → numeric)

**Silver → Gold Validations:**
- ✅ Churn rate bounds (0-100%)
- ✅ CLV positive values only
- ✅ Logical date sequences (join_date < last_login)
- ✅ Revenue consistency checks

## 📚 Documentation

Detailed documentation available in `/docs`:
- **[Architecture Guide](docs/architecture.md)**: Deep dive into medallion design
- **[Data Dictionary](docs/data_dictionary.md)**: Column-level metadata
- **[KPI Definitions](docs/kpi_definitions.md)**: Business logic and formulas
- **[Setup Guide](docs/setup_guide.md)**: Databricks configuration walkthrough

## 🔮 Future Enhancements

- [ ] Real-time streaming ingestion (Kafka → Bronze)
- [ ] ML churn prediction model (Random Forest/XGBoost)
- [ ] Tableau/Power BI dashboard integration
- [ ] A/B testing framework for reactivation campaigns
- [ ] Multi-lingual support (ES, FR, DE data sources)
- [ ] Incremental processing with Delta Lake merge

## 👤 Author

**Ron [Your Last Name]**  
Data Analytics Professional | MSc International Sports Management  
[LinkedIn](https://linkedin.com/in/yourprofile) | [Portfolio](https://yourwebsite.com) | [Email](mailto:your.email@example.com)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Dataset generated for educational/portfolio purposes
- Inspired by real-world sports analytics challenges at European football clubs
- Built with Databricks Community Edition

---

⭐ **Star this repo** if you found it helpful!  
🐛 **Issues and PRs** welcome - let's improve together!
