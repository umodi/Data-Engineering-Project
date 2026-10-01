from pyspark.sql.functions import col
from typing import Dict

def get_latest_timestamp(spark,job_id, obj_stat, execution_env,target_table):
    #if layer.lower() == "bronze":
    #    tab = f"{execution_env}_bz_phm.pd_tpdata_fdb.object_run_stats"
    #elif layer.lower() == "silver":
    #    tab = f"{execution_env}_sl_phm.pd_tpdata_fdb.object_run_stats"


    default_timestamp = "1900-01-01 00:00:00"
    result = spark.table(f"{obj_stat}")\
         .filter(f"job_id = '{job_id}' and status = 'SUCCESS' and target_object = '{target_table}'")\
             .orderBy(col("load_timestamp").desc()).limit(1).collect()

    latest_timestamp = result[0]["load_timestamp"] if result else default_timestamp

    #latest_timestamp = spark.table("tab")\
    #     .filter(f"job_name = '{job_name}' and status = 'SUCCESS'")\
    #         .orderBy(col("end_ts").desc()).limit(1).collect()[0]['end_ts']
            
    return latest_timestamp



 
