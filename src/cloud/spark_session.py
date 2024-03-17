from abc import ABC, abstractmethod
import findspark
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import shutil
from urllib.request import urlretrieve

from .storage import StorageGCP


class Spark(ABC):
    def __init__(self, *args, **kwargs):
        self.sc = None
        self.spark = None
        self.main( *args, **kwargs)

    @staticmethod
    def _set_spark_home() -> None:
        findspark.init()

    @abstractmethod
    def _get_connector() -> None:
        raise NotImplementedError

    @abstractmethod
    def _authenticate(*args, **kwargs) -> None:
        raise NotImplementedError

    @staticmethod
    def _spark_context() -> SparkContext:
        return SparkContext(conf=SparkConf())

    @staticmethod
    def _spark_session(sc: SparkContext) -> SparkSession:
        return SparkSession(sc)

    def main(self, *args, **kwargs):
        self._set_spark_home()
        self._get_connector()
        self._authenticate(*args, **kwargs)
        self.sc = self._spark_context()
        self.spark = self._spark_session(self.sc)


class SparkGCP(Spark):
    def __init__(self, gcp_storage: StorageGCP, service_principal_json_name: str):
        super().__init__(
            gcp_storage=gcp_storage, 
            service_principal_json_name=service_principal_json_name,
        )

    @staticmethod
    def _get_connector() -> None:
        connector_url = "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar"
        # Download the GCS connector for spark
        file_name = connector_url.split("/")[-1]
        urlretrieve(connector_url, file_name)
        # Copy the connector inside SPARK_HOME
        spark_home = os.getenv("SPARK_HOME")
        directory = f"{spark_home}/jars"
        destination_file = os.path.join(
            directory,
            os.path.basename(file_name)
        )
        if os.path.exists(destination_file):
            # Remove the file if it exists
            os.remove(destination_file)
        shutil.move(file_name, f"{spark_home}/jars", copy_function=shutil.copy)

    @staticmethod
    def _authenticate(
        gcp_storage: StorageGCP, 
        service_principal_json_name: str,
    ) -> None: 
        # Download the service principal json
        gcp_storage.download(
            blob_name=service_principal_json_name,
            file_path=service_principal_json_name,
        )
        # Set the environment variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            f"{os.getcwd()}\{service_principal_json_name}"
        )
