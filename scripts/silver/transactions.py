# Databricks notebook source
df_hosa = spark.read.parquet("/mnt/bronze/hosa/transactions")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/transactions")

df_merged = df_hosa.unionByName(df_hosb)

df_merged.createOrReplaceTempView("transactions")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS (
# MAGIC   SELECT
# MAGIC   CONCAT(Transactionid,'-',datasource) as transactionid_key,
# MAGIC   transactionid as src_transactionid,
# MAGIC   encounterid,
# MAGIC   patientid,
# MAGIC   providerid,
# MAGIC   deptid,
# MAGIC   visitdate,
# MAGIC   servicedate,
# MAGIC   paiddate,
# MAGIC   visittype,
# MAGIC   amount,
# MAGIC   amounttype,
# MAGIC   paidamount,
# MAGIC   claimid,
# MAGIC   payorid,
# MAGIC   procedurecode,
# MAGIC   icdcode,
# MAGIC   lineofbusiness,
# MAGIC   medicaidid,
# MAGIC   medicareid,
# MAGIC   insertdate as src_insertdate,
# MAGIC   modifieddate as src_modifieddate,
# MAGIC   datasource,
# MAGIC   CASE 
# MAGIC     WHEN encounterid IS NULL OR patientid IS NULL OR transactionid IS NULL OR visitdate IS NULL THEN TRUE
# MAGIC     ELSE FALSE
# MAGIC   END AS is_quarantined
# MAGIC   FROM transactions
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.transactions (
# MAGIC   transactionid_key STRING,
# MAGIC   src_transactionid STRING,
# MAGIC   encounterid STRING,
# MAGIC   patientid STRING,
# MAGIC   providerid STRING,
# MAGIC   deptid STRING,
# MAGIC   visitdate DATE,
# MAGIC   servicedate DATE,
# MAGIC   paiddate DATE,
# MAGIC   visittype STRING,
# MAGIC   amount DOUBLE,
# MAGIC   amounttype STRING,
# MAGIC   paidamount DOUBLE,
# MAGIC   claimid STRING,
# MAGIC   payorid STRING,
# MAGIC   procedurecode INTEGER,
# MAGIC   icdcode STRING,
# MAGIC   lineofbusiness STRING,
# MAGIC   medicaidid STRING,
# MAGIC   medicareid STRING,
# MAGIC   src_insertdate DATE,
# MAGIC   src_modifieddate DATE,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   audit_insertdate TIMESTAMP,
# MAGIC   audit_modifieddate TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.transactionid_key = source.transactionid_key
# MAGIC AND target.is_current = TRUE 
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC   target.src_transactionid <> source.src_transactionid
# MAGIC   OR target.encounterid <> source.encounterid
# MAGIC   OR target.patientid <> source.patientid
# MAGIC   OR target.providerid <> source.providerid
# MAGIC   OR target.deptid <> source.deptid
# MAGIC   OR target.visitdate <> source.visitdate
# MAGIC   OR target.servicedate <> source.servicedate
# MAGIC   OR target.paiddate <> source.paiddate
# MAGIC   OR target.visittype <> source.visittype
# MAGIC   OR target.amount <> source.amount
# MAGIC   OR target.amounttype <> source.amounttype
# MAGIC   OR target.paidamount <> source.paidamount
# MAGIC   OR target.claimid <> source.claimid
# MAGIC   OR target.payorid <> source.payorid
# MAGIC   OR target.procedurecode <> source.procedurecode
# MAGIC   OR target.icdcode <> source.icdcode
# MAGIC   OR target.lineofbusiness <> source.lineofbusiness
# MAGIC   OR target.medicaidid <> source.medicaidid
# MAGIC   OR target.medicareid <> source.medicareid
# MAGIC   OR target.src_insertdate <> source.src_insertdate
# MAGIC   OR target.src_modifieddate <> source.src_modifieddate
# MAGIC   OR target.datasource <> source.datasource
# MAGIC   OR target.is_quarantined <> source.is_quarantined
# MAGIC ) THEN
# MAGIC UPDATE SET
# MAGIC   target.is_current = FALSE,
# MAGIC   target.audit_modifieddate = current_timestamp()
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.transactions AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.transactionid_key = source.transactionid_key
# MAGIC AND target.is_current = TRUE 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC   transactionid_key,
# MAGIC   src_transactionid,
# MAGIC   encounterid,
# MAGIC   patientid,
# MAGIC   providerid,
# MAGIC   deptid,
# MAGIC   visitdate,
# MAGIC   servicedate,
# MAGIC   paiddate,
# MAGIC   visittype,
# MAGIC   amount,
# MAGIC   amounttype,
# MAGIC   paidamount,
# MAGIC   claimid,
# MAGIC   payorid,
# MAGIC   procedurecode,
# MAGIC   icdcode,
# MAGIC   lineofbusiness,
# MAGIC   medicaidid,
# MAGIC   medicareid,
# MAGIC   src_insertdate,
# MAGIC   src_modifieddate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.transactionid_key,
# MAGIC   source.src_transactionid,
# MAGIC   source.encounterid,
# MAGIC   source.patientid,
# MAGIC   source.providerid,
# MAGIC   source.deptid,
# MAGIC   source.visitdate,
# MAGIC   source.servicedate,
# MAGIC   source.paiddate,
# MAGIC   source.visittype,
# MAGIC   source.amount,
# MAGIC   source.amounttype,
# MAGIC   source.paidamount,
# MAGIC   source.claimid,
# MAGIC   source.payorid,
# MAGIC   source.procedurecode,
# MAGIC   source.icdcode,
# MAGIC   source.lineofbusiness,
# MAGIC   source.medicaidid,
# MAGIC   source.medicareid,
# MAGIC   source.src_insertdate,
# MAGIC   source.src_modifieddate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   TRUE
# MAGIC );