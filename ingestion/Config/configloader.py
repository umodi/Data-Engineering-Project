import os
import json
from typing import Dict
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


#def load_config(path: str) -> Dict:
#    with open(path, "r") as f:
#        return yaml.safe_load(f)
    
#config = load_config("dev.json")


def get_config():
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    workspace_id = workspace_url.split('.')[0]
    if workspace_id == "8259551540846499":
        env = 'dev' 
    elif workspace_id == "8259554499419819":
        env = 'stg'
    elif workspace_id == "8259552744516577":
        env = 'prod'
    cwd = os.getcwd()
    full_path = os.path.join(cwd, f"Config/{env}.json")
    with open(full_path, "r") as f:
        return json.load(f)
 
