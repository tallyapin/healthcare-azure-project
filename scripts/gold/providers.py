# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_providers (
# MAGIC   providerid STRING,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   deptid STRING,
# MAGIC   npi LONG,
# MAGIC   datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_providers

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_providers
# MAGIC SELECT
# MAGIC provider_id,
# MAGIC first_name,
# MAGIC last_name,
# MAGIC CONCAT(dept_id,'-', datasource) AS deptid,
# MAGIC npi,
# MAGIC datasource
# MAGIC FROM silver.providers
# MAGIC WHERE is_quarantined = FALSE