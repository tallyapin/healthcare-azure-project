# 🏥 Healthcare Data Engineering Project (Azure End-to-End Pipeline)

## 📘 Overview
This project demonstrates an **end-to-end healthcare data engineering pipeline** built on the Azure ecosystem. The goal is to integrate, clean, transform, and model data from multiple hospital EMR systems, payor sources, and ICD APIs into a unified **Delta Lakehouse** architecture for advanced analytics and reporting.

The final **Transaction Data Mart** (Gold layer) supports healthcare financial and operational analytics, enabling insights on patient transactions, provider performance, and claim-level payments.

---

## 🧱 Architecture Diagram
![Azure End-to-End Data Architecture](<LINK_TO_AZURE_ARCHITECTURE_IMAGE>)

### 🔄 Data Flow Summary
1. **Sources**
   - Hospital EMR systems (SQL databases)
   - Payor claim files (CSV format)
   - Public APIs for ICD codes

2. **Landing Zone**
   - Raw data is ingested into Azure Blob/ADLS as Parquet or CSV.

3. **Bronze Layer (Raw Zone)**
   - Data stored in **ADLS Gen2**.
   - Minimal transformations — schema standardization, partitioning, and format conversion.

4. **Silver Layer (Cleansed Zone)**
   - Data cleansed, joined, and conformed into Delta Tables.
   - Standardized keys, column naming conventions, and deduplication applied.

5. **Gold Layer (Curated Zone)**
   - Star-schema **Transaction Data Mart** built for analytical consumption.
   - Optimized Delta Tables support Power BI and downstream analytics.

---

## 🪙 Gold Layer: Transaction Data Mart (Star Schema)
![Transaction Data Mart Model](<LINK_TO_DATA_MODEL_IMAGE>)

### 📊 Fact Table
**`fact_transactions`**
- Central transactional table containing encounter, charge, and payment-level data.
- **Primary Key:** `transaction_id`

| Column | Description |
|--------|--------------|
| `transaction_id` | Unique transaction record ID |
| `patient_id` | Foreign key to `dim_patients` |
| `dept_id` | Foreign key to `dim_departments` |
| `provider_id` | Foreign key to `dim_providers` |
| `icd_code` | Foreign key to `dim_icd_codes` |
| `service_date` | Date of healthcare service |
| `charge_amt` | Total charged amount |
| `payor_amt_paid` | Amount paid by insurance/payor |
| `adjustment_amt` | Adjustment applied to claim |
| `patient_paid_amt` | Amount paid by patient |
| `claim_date`, `paid_date` | Key financial timestamps |

---

### 🧩 Dimension Tables

#### `dim_patients`
Stores patient demographics and source system lineage.  
Columns: `patient_id`, `first_name`, `last_name`, `dob`, `gender`, `data_source`

#### `dim_icd_codes`
Holds ICD code metadata pulled from public APIs.  
Columns: `icd_code`, `description`, `icd_type`

#### `dim_departments`
Describes hospital departments linked to providers and transactions.  
Columns: `dept_id`, `name`, `data_source`

#### `dim_providers`
Captures provider details including associated NPI and department.  
Columns: `provider_id`, `provider_name`, `npi`, `dept_id`

#### `dim_npi`
Maps NPI identifiers to provider identities and organizations.  
Columns: `npi`, `first_name`, `last_name`, `position`, `organization_name`

---

## ⚙️ Tech Stack
| Layer | Tools / Services |
|-------|------------------|
| Data Ingestion | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | Azure Databricks (PySpark, Delta Lake) |
| Data Modeling | Delta Tables, Medallion Architecture |
| Orchestration | ADF Pipelines, Databricks Jobs |
| Analytics | Power BI / SQL Endpoint |
| Version Control | GitHub + CI/CD Integration |

---

## 🚀 Key Features
- **End-to-End Data Flow:** From raw hospital systems to curated analytics-ready tables.  
- **Delta Lake Architecture:** ACID transactions and schema evolution support.  
- **Star Schema Model:** Enables performant querying and BI analysis.  
- **Automated Ingestion:** ADF pipelines orchestrate incremental and full loads.  
- **Data Quality Checks:** Null handling, schema validation, and deduplication at Silver layer.  
- **Scalability:** Modular design for onboarding additional hospitals or data domains.

---

## 🌟 About Me

Hi there! I'm **Tal Lyapin**. I am a data analyst who specializes in building data solutions, dashboards, and models that turn complex data into clear, actionable insights. Skilled in Python, SQL, Power BI, dbt, Spark, and Snowflake, I focus on using data to optimize performance and guide strategic decisions.
