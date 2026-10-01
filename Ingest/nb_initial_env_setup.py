# Databricks notebook source
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from Readers import get_reader
from Config.configloader import get_config
from Utility.Audit import start_job_audit,end_job_audit,write_object_audit,log_error
from Utility.Audit_handler import add_audit_columns,remove_audit_columns
from Utility.incremental_utils import get_latest_timestamp
import threading
from concurrent.futures import as_completed

# COMMAND ----------

dbutils.widgets.text("connection_key", "")
dbutils.widgets.text("table_metadata_file_location", "")
dbutils.widgets.text("job_metadata_file_location", "")
dbutils.widgets.text("create_job_run_stats", "")
dbutils.widgets.text("create_object_run_stats", "")
dbutils.widgets.text("create_error_log", "")
dbutils.widgets.text("create_bronze_main_tables", "")
dbutils.widgets.text("create_gold_main_tables", "")
dbutils.widgets.text("create_gold_control_table", "")
dbutils.widgets.text("run_bronze_alter_statement", "")
dbutils.widgets.text("run_gold_alter_statement", "")

connection_key = dbutils.widgets.get("connection_key")
table_metadata_file_location = dbutils.widgets.get("table_metadata_file_location")
job_metadata_file_location = dbutils.widgets.get("job_metadata_file_location")
create_job_run_stats = dbutils.widgets.get("create_job_run_stats")
create_object_run_stats = dbutils.widgets.get("create_object_run_stats")
create_error_log = dbutils.widgets.get("create_error_log")
create_bronze_main_tables = dbutils.widgets.get("create_bronze_main_tables")
create_gold_main_tables = dbutils.widgets.get("create_gold_main_tables")
create_gold_control_table = dbutils.widgets.get("create_gold_control_table")
run_bronze_alter_statement = dbutils.widgets.get("run_bronze_alter_statement")
run_gold_alter_statement = dbutils.widgets.get("run_gold_alter_statement")

# COMMAND ----------

# DBTITLE 1,extracting environment
config = get_config()
conn_cfg = config[connection_key]
execution_env = conn_cfg["env"]
print(execution_env)

# COMMAND ----------

# DBTITLE 1,reading metadata
if len(table_metadata_file_location) > 25:
    df = spark.read.csv(table_metadata_file_location, header=True, inferSchema=True, multiLine=True, escape='"')
    df.display()

# COMMAND ----------

from pyspark.sql import functions as F

def replace_env_any(df, columns, target_env):
    """
    Replace dev/stg/prd environment prefix in specified columns with target_env.
    
    Args:
        df:         Input DataFrame
        columns:    List of column names to update
        execution_env: Target environment (comes from configloader.py)
    """
    for col in columns:
        df = df.withColumn(
            col,
            F.regexp_replace(col, r"\b(dev|stg|prd)_", f"{target_env}_")
        )
    return df

# ── Config ─────────────────────────────────────────────────────────────────────
target_env     = execution_env          
cols_to_update = ["source_catalog", "target_catalog", "transformation_code"]

# ── Run ────────────────────────────────────────────────────────────────────────
if len(table_metadata_file_location) > 25:
    df_final = replace_env_any(df, cols_to_update, target_env=execution_env)
    df_final.display()

# COMMAND ----------

# DBTITLE 1,creating metadata table
if len(table_metadata_file_location) > 25:
  spark.sql(f"""
CREATE OR REPLACE TABLE {execution_env}_operations.hana_config.table_mtdt (
  id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
  source_type STRING COLLATE UTF8_BINARY,
  source_system_name STRING COLLATE UTF8_BINARY,
  source_system_dtls STRING COLLATE UTF8_BINARY,
  source_table STRING COLLATE UTF8_BINARY,
  source_schema STRING COLLATE UTF8_BINARY,
  source_catalog STRING COLLATE UTF8_BINARY,
  source_file_type STRING COLLATE UTF8_BINARY,
  source_file_path STRING COLLATE UTF8_BINARY,
  source_file_name STRING COLLATE UTF8_BINARY,
  target_type STRING COLLATE UTF8_BINARY,
  target_system_name STRING COLLATE UTF8_BINARY,
  target_system_dtls STRING COLLATE UTF8_BINARY,
  target_table STRING COLLATE UTF8_BINARY,
  target_schema STRING COLLATE UTF8_BINARY,
  target_catalog STRING COLLATE UTF8_BINARY,
  export_file_type STRING COLLATE UTF8_BINARY,
  export_file_path STRING COLLATE UTF8_BINARY,
  export_file_name STRING COLLATE UTF8_BINARY,
  export_table_name STRING COLLATE UTF8_BINARY,
  export_schema STRING COLLATE UTF8_BINARY,
  export_catalog STRING COLLATE UTF8_BINARY,
  transformation_code STRING COLLATE UTF8_BINARY,
  usecase STRING COLLATE UTF8_BINARY,
  primary_key STRING COLLATE UTF8_BINARY,
  connection_key STRING COLLATE UTF8_BINARY,
  watermark_column STRING COLLATE UTF8_BINARY,
  job_id INT,
  active_flag BOOLEAN,
  load_type STRING COLLATE UTF8_BINARY,
  flow_type STRING COLLATE UTF8_BINARY,
  scd STRING COLLATE UTF8_BINARY,
  created_by STRING COLLATE UTF8_BINARY DEFAULT CURRENT_USER(),
  src_delimiter STRING COLLATE UTF8_BINARY,
  trg_delimiter STRING COLLATE UTF8_BINARY,
  zip_details STRING COLLATE UTF8_BINARY,
  target_exclusions STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')""")


# COMMAND ----------

if len(table_metadata_file_location)>25:
    df_final.drop("id").write.mode("append").saveAsTable(f"{execution_env}_operations.hana_config.table_mtdt")
    print('table metadata table created successfully and data loaded into it')

# COMMAND ----------

# MAGIC %md
# MAGIC metadata table creation ends here, the metadata will load as per each environment on it's own. No need to change anything at all, it'll change the catalog name itself in required columns (source catalog, target catalog as well as transformation code)

# COMMAND ----------

if len(job_metadata_file_location)> 25:
  df2 = spark.read.csv(job_metadata_file_location, header=True, inferSchema=True, multiLine=True, escape='"')
  df2 = df2.drop('_rescued_data')
  df2.display()
  spark.sql(f"""
  CREATE OR REPLACE TABLE {execution_env}_operations.hana_config.job_mtdt (
    job_description STRING COLLATE UTF8_BINARY,
    job_id INT,
    job_name STRING COLLATE UTF8_BINARY)
  USING delta
  COMMENT 'Created by the file upload UI'
  TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.columnMapping' = 'supported',
    'delta.feature.deletionVectors' = 'supported',
    'delta.feature.domainMetadata' = 'supported',
    'delta.feature.identityColumns' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.feature.rowTracking' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7',
    'delta.parquet.compression.codec' = 'zstd')""")
  df2.write.mode("append").saveAsTable(f"{execution_env}_operations.hana_config.job_mtdt")
  print('job metadata table created successfully and data loaded into it')

# COMMAND ----------

if create_job_run_stats.lower() == 'y':
  spark.sql(f"""
CREATE OR REPLACE TABLE {execution_env}_operations.hana_events.job_run_stats (
  audit_id STRING COLLATE UTF8_BINARY,
  job_id STRING COLLATE UTF8_BINARY,
  layer STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  status STRING COLLATE UTF8_BINARY,
  start_ts TIMESTAMP,
  end_ts TIMESTAMP,
  triggered_by STRING COLLATE UTF8_BINARY,
  execution_env STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')""")
  print('created job run stats table successfully')


# COMMAND ----------

if create_object_run_stats.lower() == 'y':
  spark.sql(f"""
CREATE OR REPLACE TABLE {execution_env}_operations.hana_events.object_run_stats (
  audit_id STRING COLLATE UTF8_BINARY,
  job_id INT,
  mtdt_id INT,
  layer STRING COLLATE UTF8_BINARY,
  run_id STRING COLLATE UTF8_BINARY,
  source_object STRING COLLATE UTF8_BINARY,
  target_object STRING COLLATE UTF8_BINARY,
  load_type STRING COLLATE UTF8_BINARY,
  records_read BIGINT,
  records_written BIGINT,
  load_timestamp TIMESTAMP,
  status STRING COLLATE UTF8_BINARY)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')""")
  print('created object run stats table successfully')


# COMMAND ----------

if create_error_log.lower() == 'y':
  spark.sql(f"""
CREATE OR REPLACE TABLE {execution_env}_operations.hana_events.error_log (
  error_id STRING COLLATE UTF8_BINARY,
  job_id INT,
  job_run_id STRING COLLATE UTF8_BINARY,
  job_name STRING COLLATE UTF8_BINARY,
  error_message STRING COLLATE UTF8_BINARY,
  level STRING COLLATE UTF8_BINARY,
  layer STRING COLLATE UTF8_BINARY,
  error_timestamp TIMESTAMP)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')""")
  print('created error log table successfully')

 bronze_ddl = ""
 gold_ddl = ""

if create_bronze_main_tables.lower() == 'y':
    for stmt in bronze_ddl.split(';'):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)
    print('Created bronze main tables successfully')

# COMMAND ----------

if create_gold_main_tables.lower() == 'y':
    for stmt in gold_ddl.split(';'):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)
    print('Created gold main tables successfully')

# COMMAND ----------

gold_alter = f"""
Alter table {execution_env}_gd_phm.pd_prod.bskufed
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_prod.bactcls
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry.blyrinv
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_prod.bitmmas
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_purch.bpohedr
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_purch.bpodetl
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry_plan.t_forecast_entity
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pf_rfrnc.brelate
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_prod.bskufil
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry_plan.bplandt
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry_plan.t_history_weekly
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_purch.csrpus01
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry_plan.bfcstat
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_invtry_plan.bplanhd
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_prod.cssmsm02
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
Alter table {execution_env}_gd_phm.pd_prod.bdowsku
SET TAGS('medallion_layer'='gd','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
"""

# COMMAND ----------

bronze_alter = f"""
Alter table {execution_env}_bz_phm.pd_df_fcst.t_forecast_entity
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_df_fcst.t_history_weekly
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bactcls
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bdowsku
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bfcstat
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bitmmas
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.blyrinv
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bplandt
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bplanhd
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bpodetl
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bpohedr
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.brelate
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bskufed
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsfcd.bskufil
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsxcd.csrpus01
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');

Alter table {execution_env}_bz_phm.pd_score_scsxcd.cssmsm02
SET TAGS('medallion_layer'='bz','data_classification'='Cardinal Internal', 'regulated_classification'='Cardinal Internal');
"""

# COMMAND ----------

if (run_gold_alter_statement).lower() == 'y':
    for stmt in gold_alter.split(';'):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)
    print('Altered gold tables successfully, added table tags')

# COMMAND ----------

if (run_bronze_alter_statement).lower()== 'y':
    for stmt in bronze_alter.split(';'):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)
    print('Altered bronze tables successfully, added table tags')

# COMMAND ----------

if create_gold_control_table.lower() == 'y':
    spark.sql(f"""   
CREATE OR REPLACE TABLE {execution_env}_operations.hana_config.gold_control_tbl (
  table_name STRING COLLATE UTF8_BINARY,
  load_date TIMESTAMP,
  is_history BOOLEAN)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd')
 """)
    print('Created gold control table successfully, now inserting data into control table..')
    spark.sql(f"""INSERT INTO {execution_env}_operations.hana_config.gold_control_tbl VALUES
  ('BITMMAS', null, true),
  ('BLYRINV', null, true),
  ('BPLANDT', null, true),
  ('BFCSTAT', null, true),
  ('BRELATE', null, true),
  ('BPOHEDR', null, true),
  ('BPODETL', null, true),
  ('CSSMSM02', null, true),
  ('BSKUFIL', null, true),
  ('T_FORECAST_ENTITY', null, true),
  ('BSKUFED', null, true),
  ('T_HISTORY_WEEKLY', null, true),
  ('BACTCLS', null, true),
  ('CSRPUS01', null, true),
  ('BPLANHD', null, true),
  ('BDOWSKU', null, true)
 """)
