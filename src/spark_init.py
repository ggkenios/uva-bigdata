from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession

from src.cloud import SparkGCP, SparkDefault, StorageGCP
from src.config import PROJECT_ID, CLOUD_STORAGE, SERVICE_PRINCIPAL_JSON


DEFAULT = False


@dataclass
class SparkInit:
    """Initializes the spark context and session with GCP storage connector."""
    if DEFAULT:
        __spark = SparkDefault()
    else:
        __spark = SparkGCP(
            gcp_storage=StorageGCP(PROJECT_ID, CLOUD_STORAGE),
            service_principal_json_name=SERVICE_PRINCIPAL_JSON,
        )

    sc: SparkContext = __spark.sc
    spark: SparkSession = __spark.spark
