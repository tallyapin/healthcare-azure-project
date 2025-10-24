# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_departments (
# MAGIC   dept_id STRING,
# MAGIC   src_dept_id STRING,
# MAGIC   name STRING,
# MAGIC   datasource STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.dim_departments

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.dim_departments
# MAGIC SELECT DISTINCT
# MAGIC dept_id,
# MAGIC src_dept_id,
# MAGIC name,
# MAGIC datasource
# MAGIC FROM silver.departments
# MAGIC WHERE is_quarantined = FALSE