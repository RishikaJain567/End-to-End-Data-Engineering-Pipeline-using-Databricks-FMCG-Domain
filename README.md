# 🏭 End-to-End Data Engineering Pipeline using Databricks — FMCG Domain

## 📋 Overview
An end-to-end ETL pipeline built on **Databricks** for a two-company FMCG group (a parent company and its child company). The **Medallion Architecture** (Bronze → Silver → Gold) 🥉🥈🥇 is applied to the **child company's** raw sales data — ingested from **Amazon S3** ☁️ and processed through cleansing, validation, and transformation in PySpark and SQL. The resulting child Gold-layer tables are then **merged with the parent company's existing Gold-layer data** 🔗, producing a single unified, denormalized Gold dataset that powers business dashboards 📊 and natural-language querying via **Databricks Genie** 🧞.

## 🏗️ Architecture
```
☁️  Amazon S3 (child company raw data)
      │
      ▼
🥉  Bronze Layer  →  Raw ingestion, minimal transformation
      │
      ▼
🥈  Silver Layer  →  Data cleansing, validation, standardization
      │
      ▼
🥇  Gold Layer (Child)  →  Denormalized dimension & fact tables
      │
      ▼
🔗  Merge with Parent Company Gold Data  →  Unified Gold dataset
      │
      ▼
📊  Power BI Dashboards  ·  🧞  Databricks Genie (NL Querying)

⏰  Entire flow orchestrated & scheduled via Databricks Jobs & Pipelines
```

## 🛠️ Tech Stack
- 🧱 **Databricks** — pipeline development & orchestration (Databricks Jobs)
- 🐍 **PySpark** — data transformation logic
- 🗄️ **SQL** — querying and Gold-layer table design
- ☁️ **Amazon S3** — raw data storage
- 🌊 **Delta Lake** — Bronze/Silver/Gold storage format
- 🧞 **Databricks Genie** — natural-language querying for business stakeholders

## ✅ What This Pipeline Does
- 🥉🥈🥇 Applies the Bronze/Silver/Gold Medallion Architecture to the **child company's** raw sales data for reliable, incremental data quality
- 🧹 Cleans, validates, standardizes, and transforms data across **10+ dimension and fact tables**
- 🔗 Merges the child company's Gold-layer output with the **parent company's** Gold-layer data to produce one centralized, unified dataset
- ⚙️ Automates end-to-end execution using Databricks Jobs orchestration
- ⏰ Configured to run on a **scheduled basis** using Databricks Jobs & Pipelines, enabling automated, recurring refreshes without manual intervention
- 📦 Delivers a denormalized, merged Gold-layer table designed for direct BI consumption and natural-language queries

## 📂 Repository Structure
```
├── 📁 Dataset/                      # Sample/raw source data
├── 📁 Dimension-Data-Processing/    # Notebooks/scripts for building dimension tables
├── 📁 Fact-Data-Processing/         # Notebooks/scripts for building fact tables
├── 📁 setup/                        # Environment & pipeline setup scripts
└── 📄 README.md
```

## 🚀 How to Run
1. 📥 Clone this repository
2. ☁️ Upload source data to your Amazon S3 bucket (or use the sample data in `Dataset/`)
3. ⚙️ Configure your Databricks workspace and S3 connection details in `setup/`
4. ▶️ Run the notebooks in `Dimension-Data-Processing/` and `Fact-Data-Processing/` in sequence to build the Silver and Gold layers
5. 🔌 Connect Power BI or Databricks Genie to the Gold-layer tables for reporting

## 🎯 Key Outcomes
- 🏢 Onboarded the child company's data onto the parent company's Gold-layer model through a governed, repeatable Medallion pipeline
- ✨ Improved data quality and consistency through systematic validation at each Medallion layer before merging
- 🔍 Enabled a single unified view across parent and child company data for BI and self-service, natural-language business queries via Databricks Genie
- ⏰ Eliminated manual pipeline runs by scheduling automated, recurring execution through Databricks Jobs & Pipelines
