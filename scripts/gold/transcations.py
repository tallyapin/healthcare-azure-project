# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.fact_transactions (
# MAGIC   transactionid_key STRING,
# MAGIC   src_transactionid STRING,
# MAGIC   encounterid STRING,
# MAGIC   fk_patientid STRING,
# MAGIC   fk_providerid STRING,
# MAGIC   fk_deptid STRING,
# MAGIC   icdcode STRING,
# MAGIC   procedurecode STRING,
# MAGIC   visittype STRING,
# MAGIC   servicedate DATE,
# MAGIC   paiddate DATE,
# MAGIC   amount DOUBLE,
# MAGIC   paidamount DOUBLE,
# MAGIC   amounttype STRING,
# MAGIC   claimid STRING,
# MAGIC   datasource STRING,
# MAGIC   refreshed_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE gold.fact_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.fact_transactions
# MAGIC SELECT
# MAGIC transactionid_key,
# MAGIC src_transactionid,
# MAGIC encounterid,
# MAGIC CONCAT(patientid,'-', datasource) AS fk_patientid,
# MAGIC CASE WHEN datasource='hos-a' THEN CONCAT('H1-',providerID) else CONCAT('H2-',providerID ) END AS fk_providerid, 
# MAGIC CONCAT(deptid,'-',datasource ) as fk_deptid, 
# MAGIC icdcode,
# MAGIC procedurecode,
# MAGIC visittype,
# MAGIC servicedate,
# MAGIC paiddate,
# MAGIC amount,
# MAGIC paidamount,
# MAGIC amounttype,
# MAGIC claimid,
# MAGIC datasource,
# MAGIC current_timestamp()
# MAGIC FROM silver.transactions
# MAGIC WHERE is_current = TRUE AND is_quarantined = FALSE