from .base_reader import BaseReader
import pandas as pd
import os
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.dbutils import DBUtils
from google.cloud import storage
import zipfile
from datetime import datetime

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class FileReader(BaseReader):

    # Reads CSV files from GCS (optionally from ZIP archives) and loads them into Databricks.Supports both plain CSV and ZIP-compressed CSV files.


    def read(self, spark, row, connection_key):

        #Read CSV files from GCS into a PySpark DataFrame

        
        conn_cfg = self.config[connection_key]
        
        # Get GCS configuration
        gcs_path = conn_cfg.get("gcs_path")
        file_name = row.get("source_table")
        is_zipped = conn_cfg.get("is_zipped", False)
        delimiter = row.get("delimiter", ",")
        
        # Construct full GCS path
        file_extension = ".csv.zip" if is_zipped else ".csv"
        full_gcs_path = f"{gcs_path}/{file_name}{file_extension}"
        
        try:
            # Download and extract if needed
            temp_dir = tempfile.mkdtemp()
            local_file_path = self._download_from_gcs(full_gcs_path, temp_dir, is_zipped)
            
            # Read CSV into DataFrame
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", delimiter)
                .csv(local_file_path)
            )
            
            # Add metadata columns
            df = df.withColumn("load_timestamp", current_timestamp()) \
                   .withColumn("source_system", lit(conn_cfg.get("source_system"))) \
                   .withColumn("source_file", lit(full_gcs_path))
            
            # Log the load
            self._log_load(spark, row, conn_cfg, local_file_path)
            
            return df
            
        finally:
            # Cleanup temp directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
    
    def _download_from_gcs(self, gcs_path, temp_dir, is_zipped=False):

        #Download file from GCS and extract if zipped

        
        try:
            # Parse GCS path (gs://bucket/path/to/file)
            parts = gcs_path.replace("gs://", "").split("/", 1)
            bucket_name = parts[0]
            blob_path = parts[1]
            
            # Initialize GCS client
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            
            # Download file
            local_download_path = os.path.join(temp_dir, os.path.basename(blob_path))
            blob.download_to_filename(local_download_path)
            
            # Extract if zipped
            if is_zipped:
                extract_dir = os.path.join(temp_dir, "extracted")
                os.makedirs(extract_dir, exist_ok=True)
                
                with zipfile.ZipFile(local_download_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                # Find the CSV file inside
                csv_files = [f for f in os.listdir(extract_dir) if f.endswith('.csv')]
                if csv_files:
                    return os.path.join(extract_dir, csv_files[0])
                else:
                    raise FileNotFoundError(f"No CSV file found inside {gcs_path}")
            else:
                return local_download_path
                
        except Exception as e:
            raise Exception(f"Error downloading/extracting file from GCS: {gcs_path}. Error: {str(e)}")
    
    def _log_load(self, spark, row, conn_cfg, local_file_path):

        #Log the file load to tracking table

        
        try:
            # Get file size
            file_size = os.path.getsize(local_file_path)
            
            # Prepare load info
            load_info = {
                "load_timestamp": datetime.now(),
                "source_table": row.get("source_table"),
                "target_table": row.get("target_table"),
                "source_system": conn_cfg.get("source_system"),
                "load_type": "SNAPSHOT",
                "file_path": local_file_path,
                "file_size_bytes": file_size,
                "status": "SUCCESS"
            }
            
            # Log to load_info table if configured
            if "load_info" in conn_cfg:
                print(f"Load logged: {load_info}")
            
        except Exception as e:
            print(f"Warning: Could not log load information. Error: {str(e)}")
