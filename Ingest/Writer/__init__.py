from .dbx_writer import DBXWriter
from .vendavo_writer import VendavoWriter
from .boss_writer import BossWriter
from .boss_writer_cdc import BossWriter_cdc

WRITER_REGISTRY = {
    "lexisnexis": DBXWriter,
    "databricks": DBXWriter,
    "vendavo": VendavoWriter,
    "boss": BossWriter,
    "boss_cdc": BossWriter_cdc,
}


def get_writer(usecase: str, spark, config):
    usecase = usecase.lower()

    if usecase not in WRITER_REGISTRY:
        raise ValueError(f"Unsupported source type: {usecase}")

    return WRITER_REGISTRY[usecase](spark, config)
