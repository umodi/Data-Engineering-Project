from abc import ABC, abstractmethod


class BaseReader(ABC):


    def __init__(self, spark, config: dict):
        self.spark = spark
        self.config = config

    @abstractmethod
    def read(self, metadata_row):
        pass
 
