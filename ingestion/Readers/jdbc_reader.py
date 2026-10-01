from .base_reader import BaseReader
import pandas as pd
import shutil
import tempfile
import os
import ssl
import subprocess
import stat
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.dbutils import DBUtils

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

class JDBCReader(BaseReader):

    def read(self,spark, row,connection_key):

        conn_cfg = self.config[connection_key]

        load_timestamp = spark.table(conn_cfg["load_info"]) \
            .filter(col("target_table") == row["target_table"]) \
                .collect()[0]['load_timestamp']

        database_table = (
                f"{row['source_schema']}.{row['source_table']}"
                if row.get("source_schema")
                else row["source_table"]
            )
        
        jdbc_url = f"jdbc:{conn_cfg['type']}://{conn_cfg['jdbc_hostname']}:{conn_cfg['jdbc_port']}/{conn_cfg['jdbc_database']}"

        jdbc_username = dbutils.secrets.get(scope=conn_cfg["scope"], key=conn_cfg["username_key"])
        jdbc_password = dbutils.secrets.get(scope=conn_cfg["scope"], key=conn_cfg["password_key"])

        df = (
        spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", f"{row['tranformation_code']} where Row_update_stp >= '{load_timestamp}') AS t")
                .option("user", jdbc_username)
                .option("password", jdbc_password)
                .option("driver", conn_cfg["driver"])
                .load()
        )

        
        return df
        #display(sdf)
