from .base_reader import BaseReader

class BlobReader(BaseReader):

    def read(self, row):

        conn_key = row["connection_key"]
        blob_cfg = self.config["blob"][conn_key]

        file_format = row.get("file_format", "parquet")
        full_path = f"{blob_cfg['base_path']}/{row['source_table']}"

        df = (
            self.spark.read
                .format(file_format)
                .load(full_path)
        )

        return df
