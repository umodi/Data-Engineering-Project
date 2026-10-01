from .base_reader import BaseReader
import pandas as pd
import shutil
import tempfile
import os
import ssl
import subprocess
import stat

class DBXReader(BaseReader):

    def read(self,spark, row,connection_key):
        conn_cfg = self.config[connection_key]

        transformation = row.get("transformation_code")

        if transformation.lower() == 'null' or transformation is None:
            database_table = f"{row['source_catalog']}.{row['source_schema']}.{row['source_table']}"
            df = spark.table(database_table)

        else:
            df = spark.sql(transformation)
        return df

 
