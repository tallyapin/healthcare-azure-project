# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

df_hosa = spark.read.parquet("/mnt/bronze/hosa/departments")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/departments")

df_merged = df_hosa.unionByName(df_hosb)

df_merged = df_merged.withColumn("src_dept_id", f.col("deptid")) \
                     .withColumn("dept_id", f.concat(f.col("deptid"), f.lit('-'), f.col("datasource"))) \
                     .drop("deptid")

df_merged.createOrReplaceTempView("departments")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments (
# MAGIC   dept_id string,
# MAGIC   src_dept_id string,
# MAGIC   name string,
# MAGIC   datasource string,
# MAGIC   is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver.departments
# MAGIC SELECT
# MAGIC dept_id,
# MAGIC src_dept_id,
# MAGIC name,
# MAGIC datasource,
# MAGIC CASE 
# MAGIC   WHEN src_dept_id IS NULL OR name IS NULL THEN true 
# MAGIC   ELSE false END AS is_quarantined
# MAGIC FROM departments