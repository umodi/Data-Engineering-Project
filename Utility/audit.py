import uuid
from datetime import datetime
from pyspark.sql.functions import col
import json

def start_job_audit(
    spark,
    job_id: str,
    layer: str,
    run_id: str,
    execution_env: str ,
    run_stat: str,
    triggered_by: str = "system"
) -> str:
    audit_id = str(uuid.uuid4())



    spark.sql(f"""
        INSERT INTO {run_stat} VALUES (
            '{audit_id}',
            '{job_id}',
            '{layer}',
            '{run_id}',
            'STARTED',
            current_timestamp(),
            NULL,
            '{triggered_by}',
            '{execution_env}')
    """)
    return audit_id

def end_job_audit(
    spark,
    audit_id: str,
    status: str,
    layer: str,
    run_stat: str,
    obj_stat: str
):

        
    fail_check = spark.sql(f"""
            SELECT COUNT(*) AS failed_count
            FROM {obj_stat}
            WHERE audit_id = '{audit_id}'
            AND lower(Status) = 'failed' """).collect()[0]["failed_count"]
    
    if fail_check > 0:
        status = 'FAILED'
    

    spark.sql(f"""
        UPDATE {run_stat}
        SET
            status = '{status}',
            end_ts = current_timestamp()
        WHERE audit_id = '{audit_id}'
    """)


def reconcile_record(records_read: int, rows_written: int) -> str:
    if rows_written != records_read:
        return "FAILED"
    return "SUCCESS"
   
def write_object_audit(
    spark,
    audit_id: str,
    job_id: str,
    layer: str,
    run_id: str,
    source_object: str,
    target_object: str,
    load_type: str,
    records_read: int,
    target_table: str,
    status: str,
    obj_stat: str,
    metadata_id: str

  
):
        
    details_df = spark.sql(f"select * from (describe history {target_table}) order by timestamp desc limit 1")
    temp_details = details_df.select("operationMetrics").collect()
    row = temp_details[0]
    metrics = row["operationMetrics"] or {}
    rows_written = int(metrics.get("numOutputRows", 0)) # for bronze layer reconciliation
    
    if status.lower() == 'success':

        if layer.lower() =='bronze':
            #tab_1 = 'dev_bz_phm.pd_tpdata_fdb.object_run_stats'
            status = reconcile_record(records_read,rows_written)
            
        elif layer.lower()== 'silver':
            #tab_1 = 'dev_sv_phm.pd_tpdata_product.object_run_stats'
            rows_count = spark.sql(f"""
                SELECT COUNT(*) AS active_count
                FROM {target_table}
                WHERE sv_delt_flg = 'N'
                AND sv_curr_vrsn_flg = 'Y' """).collect()[0]["active_count"]
                
            status = reconcile_record(records_read,rows_count)
    else:
        if status.lower() == "failed":
            rows_written = 0

    spark.sql(f"""
        INSERT INTO {obj_stat} VALUES (
            '{audit_id}',
            '{job_id}',
            '{metadata_id}',
            '{layer}',
            '{run_id}',
            '{source_object}',
            '{target_object}',
            '{load_type}',
            '{records_read}',
            '{rows_written}',
            current_timestamp(),
            '{status}'
        )
    """)
 

def log_error(spark, error_tab, error_message, job_name, job_id, job_run_id, level, layer):

    error_id = "err_" + str(uuid.uuid4())

    data = [(
        error_id,
        job_id,
        job_run_id,
        job_name,
        error_message,
        level,
        layer,
        datetime.now()
    )]

    columns = [
        "error_id",
        "job_id",
        "job_run_id",
        "job_name",
        "error_message",
        "level",
        "layer",
        "error_timestamp"
    ]

    df = spark.createDataFrame(data, columns)

  
    target_schema = spark.table(error_tab).schema

    for field in target_schema:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    df = df.select([field.name for field in target_schema])

    df.write.mode("append").saveAsTable(error_tab)
 
