READER_REGISTRY = {
    "mdm": ("jdbc_reader", "JDBCReader"),
    "blob": ("blob_reader", "BlobReader"),
    "dbx": ("dbx_reader", "DBXReader"),
    "databricks": ("dbx_reader", "DBXReader"),
    "hana": ("dbx_reader", "DBXReader"),
}


def get_reader(source_type: str, spark, config):
    source_type = source_type.lower()

    if source_type not in READER_REGISTRY:
        raise ValueError(f"Unsupported source type: {source_type}")

    module_name, class_name = READER_REGISTRY[source_type]
    module = __import__(f"{__name__}.{module_name}", fromlist=[class_name])
    reader_class = getattr(module, class_name)
    return reader_class(spark, config)
