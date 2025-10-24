# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/providers")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/providers")

df_merged = df_hosa.unionByName(df_hosb)


df_merged.createOrReplaceTempView("providers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC   provider_id string,
# MAGIC   first_name string,
# MAGIC   last_name string,
# MAGIC   specialization string,
# MAGIC   dept_id string,
# MAGIC   npi long,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.providers;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.providers
# MAGIC SELECT
# MAGIC DISTINCT
# MAGIC ProviderID,
# MAGIC FirstName,
# MAGIC LastName,
# MAGIC Specialization,
# MAGIC DeptID,
# MAGIC NPI,
# MAGIC datasource string,
# MAGIC CASE WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE 
# MAGIC   ELSE FALSE
# MAGIC   END AS is_quarantined
# MAGIC FROM
# MAGIC providers;