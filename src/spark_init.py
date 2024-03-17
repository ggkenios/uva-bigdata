from abc import ABC
from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession

from src.cloud import SparkGCP, StorageGCP, Storage, Spark
from src.config import PROJECT_ID, CLOUD_STORAGE, SERVICE_PRINCIPAL_JSON


@dataclass
class ConfigCloud(ABC):
    storage: Storage
    sc: SparkContext
    spark: SparkSession


@dataclass
class ConfigGCP(ConfigCloud):
    storage: Storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    __spark: Spark = SparkGCP(
        gcp_storage=storage,
        service_principal_json_name=SERVICE_PRINCIPAL_JSON,
    )
    sc: SparkContext = __spark.sc
    spark: SparkSession = __spark.spark
