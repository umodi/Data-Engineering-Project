from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit
from typing import List
import ast
 
 
def add_audit_columns(df, run_id, key_columns, layer, source_system) -> DataFrame:
    """
    Adds:
      1. composite_hash -> xxhash64 of key columns
      2. row_hash       -> xxhash64 of non-key columns
 
    :param df: Input PySpark DataFrame
    :param key_columns: List of primary key column names
    :return: DataFrame with audit columns added
    """
 
    # Validate columns
    if layer.lower() == 'bronze':
        if key_columns is None or str(key_columns).strip() == "":
            keys = []
        else:
            keys = ast.literal_eval(key_columns)
 
        df_columns = df.columns
 
        if keys:
            # Validate keys exist
            missing_keys = set(k.lower() for k in keys) - set(c.lower() for c in df_columns)
            if missing_keys:
                raise ValueError(f"Key columns not found in DataFrame: {missing_keys}")
 
            key_columns_sorted = sorted(keys)
 
            non_key_columns_sorted = sorted(
                [c for c in df_columns if c not in keys]
            )
 
            # Build composite_hash
            composite_hash_col = F.xxhash64(
                *[F.col(c).cast("string") for c in key_columns_sorted]
            ).alias("composite_hash")
 
        else:
            # No keys case
            key_columns_sorted = []
 
            # All columns become non-key columns
            non_key_columns_sorted = sorted(df_columns)
 
            # composite_hash = NULL
            composite_hash_col = F.lit('NA').alias("composite_hash")
 
        # Build row_hash (non-key hash)
        # If there are no non-key columns, hash a constant
        if non_key_columns_sorted:
            row_hash_col = F.xxhash64(
                *[F.col(c).cast("string") for c in non_key_columns_sorted]
            ).alias("row_hash")
        else:
            row_hash_col = F.xxhash64(F.lit("")).alias("row_hash")
 
        return (
            df
            .withColumn("composite_hash", composite_hash_col.cast("string"))
            .withColumn("row_hash", row_hash_col.cast("string"))
            .withColumn("bz_add_stp", F.current_timestamp())
            .withColumn("bz_run_id", lit(run_id))
        )
 
    elif layer.lower() == 'silver':
        return (
            df.withColumn("sv_start_stp", F.current_timestamp())
            .withColumn("sv_end_stp", F.lit("9999-12-31 23:59:59").cast("timestamp"))
            .withColumn("sv_add_stp", F.current_timestamp())
            .withColumn("sv_add_user_id", F.current_user())
            .withColumn("sv_update_stp", F.current_timestamp())
            .withColumn("sv_update_user_id", F.current_user())
            .withColumn("sv_delt_flg", F.lit('N'))
            .withColumn("sv_curr_vrsn_flg", F.lit('Y'))
        )
    else:
        return df
 
 
def remove_audit_columns(df, layer):
    if layer.lower() == "silver":
        df = df.drop(
            "bz_start_stp",
            "bz_end_stp",
            "bz_add_stp",
            "bz_add_user_id",
            "bz_update_stp",
            "bz_update_user_id",
            "bz_source_id",
        )
        return df
    elif layer.lower() == "gold":
        df = df.drop(
            "composite_hash",
            "row_hash",
            "sv_start_stp",
            "sv_end_stp",
            "sv_add_stp",
            "sv_add_user_id",
            "sv_update_stp",
            "sv_update_user_id",
            "sv_delt_flg",
            "sv_curr_vrsn_flg",
        )
        return df
    elif layer.lower() == "bronze":
        return df
