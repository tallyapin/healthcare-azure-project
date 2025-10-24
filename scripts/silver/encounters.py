# Databricks notebook source
df_hosa = spark.read.parquet("/mnt/bronze/hosa/encounters")
df_hosa.createOrReplaceTempView("encounters_hosa")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/encounters")
df_hosb.createOrReplaceTempView("encounters_hosb")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_encounters AS (
# MAGIC     SELECT * FROM encounters_hosa
# MAGIC     UNION ALL
# MAGIC     SELECT * from encounters_hosb
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS (
# MAGIC     SELECT
# MAGIC     CONCAT(encounterid,'-', datasource) AS encounter_key,
# MAGIC     encounterid AS src_encounterid,
# MAGIC     patientid,
# MAGIC     encounterdate,
# MAGIC     encountertype,
# MAGIC     providerid,
# MAGIC     departmentid,
# MAGIC     procedurecode,
# MAGIC     inserteddate AS src_inserteddate,
# MAGIC     modifieddate AS src_modifieddate,
# MAGIC     datasource,
# MAGIC     CASE
# MAGIC       WHEN encounterid IS NULL OR patientid IS NULL THEN TRUE
# MAGIC       ELSE FALSE
# MAGIC       END AS is_quarantined 
# MAGIC     FROM cdm_encounters
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.encounters (
# MAGIC   encounter_key STRING,
# MAGIC   src_encounterid STRING,
# MAGIC   patientid STRING,
# MAGIC   encounterdate DATE,
# MAGIC   encountertype STRING,
# MAGIC   providerid STRING,
# MAGIC   departmentid STRING,
# MAGIC   procedurecode STRING,
# MAGIC   src_inserteddate DATE,
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
# MAGIC MERGE INTO silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.encounter_key = source.encounter_key
# MAGIC AND target.is_current = TRUE 
# MAGIC WHEN MATCHED
# MAGIC AND (
# MAGIC   target.src_encounterid <> source.src_encounterid
# MAGIC   OR target.patientid <> source.patientid
# MAGIC   OR target.encounterdate <> source.encounterdate
# MAGIC   OR target.encountertype <> source.encountertype
# MAGIC   OR target.providerid <> source.providerid
# MAGIC   OR target.departmentid <> source.departmentid
# MAGIC   OR target.procedurecode <> source.procedurecode
# MAGIC   OR target.src_inserteddate <> source.src_inserteddate
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
# MAGIC MERGE INTO silver.encounters AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.encounter_key = source.encounter_key
# MAGIC AND target.is_current = TRUE 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT (
# MAGIC   encounter_key,
# MAGIC   src_encounterid,
# MAGIC   patientid,
# MAGIC   encounterdate,
# MAGIC   encountertype,
# MAGIC   providerid,
# MAGIC   departmentid,
# MAGIC   procedurecode,
# MAGIC   src_inserteddate,
# MAGIC   src_modifieddate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   audit_insertdate,
# MAGIC   audit_modifieddate,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.encounter_key,
# MAGIC   source.src_encounterid,
# MAGIC   source.patientid,
# MAGIC   source.encounterdate,
# MAGIC   source.encountertype,
# MAGIC   source.providerid,
# MAGIC   source.departmentid,
# MAGIC   source.procedurecode,
# MAGIC   source.src_inserteddate,
# MAGIC   source.src_modifieddate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   TRUE
# MAGIC );