# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_npi (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   refreshed_at TIMESTAMP)

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_npi

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_npi
# MAGIC SELECT
# MAGIC npi_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC position,
# MAGIC organisation_name,
# MAGIC last_updated,
# MAGIC current_timestamp()
# MAGIC FROM silver.npi_extract
# MAGIC WHERE is_current_flag = TRUE