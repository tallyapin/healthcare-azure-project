# Databricks notebook source
### extract patients data from hosa and hosb bronze parquet files

### create CDM to match columnm headers

### implement SCD2 to track changes to patients

df_hosa = spark.read.parquet("/mnt/bronze/hosa/patients")
df_hosa.createOrReplaceTempView("patients_hosa")

df_hosb = spark.read.parquet("/mnt/bronze/hosb/patients")
df_hosb.createOrReplaceTempView("patients_hosb")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW cdm_patients AS (
# MAGIC   SELECT CONCAT(src_patientid,'-', datasource) AS patient_key, *
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC     patientid AS src_patientid,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     middlename,
# MAGIC     ssn,
# MAGIC     phonenumber,
# MAGIC     gender,
# MAGIC     dob,
# MAGIC     address,
# MAGIC     modifieddate,
# MAGIC     datasource
# MAGIC     FROM patients_hosa
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC     id AS src_patientid,
# MAGIC     f_name AS firstname,
# MAGIC     l_name AS lastname,
# MAGIC     m_name AS middlename,
# MAGIC     ssn,
# MAGIC     phonenumber,
# MAGIC     gender,
# MAGIC     dob,
# MAGIC     address,
# MAGIC     updated_date AS modifieddate,
# MAGIC     datasource
# MAGIC     FROM patients_hosb
# MAGIC   )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS (
# MAGIC   SELECT
# MAGIC     patient_key,
# MAGIC     src_patientid,
# MAGIC     firstname,
# MAGIC     lastname,
# MAGIC     middlename,
# MAGIC     ssn,
# MAGIC     phonenumber,
# MAGIC     gender,
# MAGIC     dob,
# MAGIC     address,
# MAGIC     modifieddate AS src_modifieddate,
# MAGIC     datasource,
# MAGIC     CASE 
# MAGIC       WHEN src_patientid IS NULL OR dob IS NULL OR firstname IS NULL OR lower(firstname) IS NULL THEN TRUE 
# MAGIC       ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC   FROM cdm_patients
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.patients (
# MAGIC   patient_key STRING,
# MAGIC   src_patientid STRING,
# MAGIC   firstname STRING,
# MAGIC   lastname STRING,
# MAGIC   middlename STRING,
# MAGIC   ssn STRING,
# MAGIC   phonenumber STRING,
# MAGIC   gender STRING,
# MAGIC   dob DATE,
# MAGIC   address STRING,
# MAGIC   src_modifieddate TIMESTAMP,
# MAGIC   datasource STRING,
# MAGIC   is_quarantined BOOLEAN,
# MAGIC   inserted_date TIMESTAMP,
# MAGIC   modified_date TIMESTAMP,
# MAGIC   is_current BOOLEAN
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.patient_key = source.patient_key
# MAGIC AND target.is_current = TRUE
# MAGIC WHEN MATCHED
# MAGIC AND ( 
# MAGIC   target.src_patientid <> source.src_patientid 
# MAGIC   OR target.firstname <> source.firstname 
# MAGIC   OR target.lastname <> source.lastname 
# MAGIC   OR target.middlename <> source.middlename 
# MAGIC   OR target.ssn <> source.ssn 
# MAGIC   OR target.phonenumber <> source.phonenumber 
# MAGIC   OR target.gender <> source.gender 
# MAGIC   OR target.dob <> source.dob 
# MAGIC   OR target.address <> source.address 
# MAGIC   OR target.src_modifieddate <> source.src_modifieddate 
# MAGIC   OR target.datasource <> source.datasource
# MAGIC   OR target.is_quarantined <> source.is_quarantined
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC   target.is_current = FALSE,
# MAGIC   target.modified_date = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC   patient_key,
# MAGIC   src_patientid,
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   middlename,
# MAGIC   ssn,
# MAGIC   phonenumber,
# MAGIC   gender,
# MAGIC   dob,
# MAGIC   address,
# MAGIC   src_modifieddate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   inserted_date,
# MAGIC   modified_date,
# MAGIC   is_current
# MAGIC ) 
# MAGIC VALUES (
# MAGIC   source.patient_key,
# MAGIC   source.src_patientid,
# MAGIC   source.firstname,
# MAGIC   source.lastname,
# MAGIC   source.middlename,
# MAGIC   source.ssn,
# MAGIC   source.phonenumber,
# MAGIC   source.gender,
# MAGIC   source.dob,
# MAGIC   source.address,
# MAGIC   source.src_modifieddate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   TRUE
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.patients AS target
# MAGIC USING quality_checks AS source
# MAGIC ON target.patient_key = source.patient_key
# MAGIC AND target.is_current = TRUE
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC   patient_key,
# MAGIC   src_patientid,
# MAGIC   firstname,
# MAGIC   lastname,
# MAGIC   middlename,
# MAGIC   ssn,
# MAGIC   phonenumber,
# MAGIC   gender,
# MAGIC   dob,
# MAGIC   address,
# MAGIC   src_modifieddate,
# MAGIC   datasource,
# MAGIC   is_quarantined,
# MAGIC   inserted_date,
# MAGIC   modified_date,
# MAGIC   is_current
# MAGIC )
# MAGIC VALUES (
# MAGIC   source.patient_key,
# MAGIC   source.src_patientid,
# MAGIC   source.firstname,
# MAGIC   source.lastname,
# MAGIC   source.middlename,
# MAGIC   source.ssn,
# MAGIC   source.phonenumber,
# MAGIC   source.gender,
# MAGIC   source.dob,
# MAGIC   source.address,
# MAGIC   source.src_modifieddate,
# MAGIC   source.datasource,
# MAGIC   source.is_quarantined,
# MAGIC   current_timestamp(),
# MAGIC   current_timestamp(),
# MAGIC   TRUE
# MAGIC );