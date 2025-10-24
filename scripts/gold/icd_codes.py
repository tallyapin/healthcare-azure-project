# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_icd_codes (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_icd_codes

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_icd_codes
# MAGIC SELECT DISTINCT
# MAGIC   icd_code,
# MAGIC   icd_code_type,
# MAGIC   code_description,
# MAGIC   current_timestamp() refreshed_at
# MAGIC FROM silver.icd_codes
# MAGIC WHERE is_current_flag = TRUE