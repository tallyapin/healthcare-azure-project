# Databricks notebook source
claims_df = spark.read.parquet("/mnt/bronze/claims")

claims_df.createOrReplaceTempView("claims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS (
# MAGIC   SELECT
# MAGIC   CONCAT(claimid,'-', datasource) AS claimid_key,
# MAGIC   claimid AS src_claimid,
# MAGIC   transactionid,
# MAGIC   patientid,
# MAGIC   encounterid,
# MAGIC   providerid,
# MAGIC   deptid,
# MAGIC   CAST(servicedate AS DATE) AS service_date,
# MAGIC   CAST(claimdate AS DATE) AS claimdate,
# MAGIC   payorid,
# MAGIC   claimamount,
# MAGIC   paidamount,
# MAGIC   claimstatus,
# MAGIC   payortype,
# MAGIC   deductible,
# MAGIC   coinsurance,
# MAGIC   copay,
# MAGIC   CAST(insertdate AS DATE) AS src_insertdate,
# MAGIC   CAST(modifieddate AS DATE) AS src_modifieddate,
# MAGIC   datasource,
# MAGIC   CASE
# MAGIC     WHEN claimid IS NULL OR transactionid IS NULL OR patientid IS NULL OR service_date IS NULL THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_quarantined
# MAGIC   FROM claims
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.claims (
# MAGIC   claimid_key STRING,
# MAGIC   src_claimid STRING,
# MAGIC   transactionid STRING,
# MAGIC   patientid STRING,
# MAGIC   encounterid STRING,
# MAGIC   providerid STRING,
# MAGIC   deptid STRING,
# MAGIC   service_date DATE,
# MAGIC   claimdate DATE,
# MAGIC   payorid STRING,
# MAGIC   claimamount DOUBLE,
# MAGIC   paidamount DOUBLE,
# MAGIC   claimstatus STRING,
# MAGIC   payortype STRING,
# MAGIC   deductible DOUBLE,
# MAGIC   coinsurance DOUBLE,
# MAGIC   copay DOUBLE,
# MAGIC   src_insertdate DATE,
# MAGIC   src_modifieddate DATE,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   audit_insertdate TIMESTAMP,
# MAGIC   audit_modifieddate TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC MERGE INTO silver.claims AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.claimid_key = source.claimid_key
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC   target.src_claimid <> source.src_claimid
# MAGIC   OR target.transactionid <> source.transactionid
# MAGIC   OR target.patientid <> source.patientid
# MAGIC   OR target.encounterid <> source.encounterid
# MAGIC   OR target.providerid <> source.providerid
# MAGIC   OR target.deptid <> source.deptid
# MAGIC   OR target.service_date <> source.service_date
# MAGIC   OR target.claimdate <> source.claimdate
# MAGIC   OR target.payorid <> source.payorid
# MAGIC   OR target.claimamount <> source.claimamount
# MAGIC   OR target.paidamount <> source.paidamount
# MAGIC   OR target.claimstatus <> source.claimstatus
# MAGIC   OR target.payortype <> source.payortype
# MAGIC   OR target.deductible <> source.deductible
# MAGIC   OR target.coinsurance <> source.coinsurance
# MAGIC   OR target.src_insertdate <> source.src_insertdate
# MAGIC   OR target.src_modifieddate <> source.src_modifieddate
# MAGIC   OR target.datasource <> source.datasource
# MAGIC   OR target.is_quarantined <> source.is_quarantined
# MAGIC )
# MAGIC THEN 
# MAGIC UPDATE SET
# MAGIC   target.is_current = FALSE,
# MAGIC   target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.claims AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.claimid_key = source.claimid_key
# MAGIC AND target.is_current = TRUE
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (
# MAGIC     claimid_key,
# MAGIC     src_claimid,
# MAGIC     transactionid,
# MAGIC     patientid,
# MAGIC     encounterid,
# MAGIC     providerid,
# MAGIC     deptid,
# MAGIC     service_date,
# MAGIC     claimdate,
# MAGIC     payorid,
# MAGIC     claimamount,
# MAGIC     paidamount,
# MAGIC     claimstatus,
# MAGIC     payortype,
# MAGIC     deductible,
# MAGIC     coinsurance,
# MAGIC     copay,
# MAGIC     src_insertdate,
# MAGIC     src_modifieddate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current
# MAGIC   ) 
# MAGIC   VALUES (
# MAGIC     source.claimid_key,
# MAGIC     source.src_claimid,
# MAGIC     source.transactionid,
# MAGIC     source.patientid,
# MAGIC     source.encounterid,
# MAGIC     source.providerid,
# MAGIC     source.deptid,
# MAGIC     source.service_date,
# MAGIC     source.claimdate,
# MAGIC     source.payorid,
# MAGIC     source.claimamount,
# MAGIC     source.paidamount,
# MAGIC     source.claimstatus,
# MAGIC     source.payortype,
# MAGIC     source.deductible,
# MAGIC     source.coinsurance,
# MAGIC     source.copay,
# MAGIC     source.src_insertdate,
# MAGIC     source.src_modifieddate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     TRUE
# MAGIC   )