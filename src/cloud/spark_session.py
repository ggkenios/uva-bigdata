from abc import ABC, abstractmethod
import findspark
import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import shutil
from urllib.request import urlretrieve

from src.cloud.storage import Storage
from src.config import INITIAL_RUN


class Spark(ABC):
    def __init__(self, *args, **kwargs):
        self.sc = None
        self.spark = None
        self.initialize( *args, **kwargs)

    @staticmethod
    def _set_spark_home() -> None:
        """Sets the SPARK_HOME environment variable."""
        findspark.init()

    @abstractmethod
    def _get_connector() -> None:
        """Downloads the connectors for the cloud provider's storage."""
        raise NotImplementedError

    @abstractmethod
    def _authenticate(*args, **kwargs) -> None:
        """Authenticates the cloud provider's storage."""
        raise NotImplementedError

    @staticmethod
    def _spark_context() -> SparkContext:
        """Creates a spark context object."""
        return SparkContext(conf=SparkConf())

    @staticmethod
    def _spark_session(sc: SparkContext) -> SparkSession:
        """Creates a spark session object."""
        return SparkSession(sc)

    def initialize(self, *args, **kwargs):
        """Main method to be called by the constructor."""
        self._set_spark_home()
        if INITIAL_RUN: self._get_connector()
        self._authenticate(*args, **kwargs)
        self.sc = self._spark_context()
        self.spark = self._spark_session(self.sc)


class SparkDefault(Spark):
    def __init__(self):
        super().__init__()

    @staticmethod
    def _get_connector() -> None:
        """Empty method."""
        pass

    @staticmethod
    def _authenticate() -> None:
        """Empty method."""
        pass


class SparkGCP(Spark):
    def __init__(self, gcp_storage: Storage, service_principal_json_name: str):
        """
        Args:
            gcp_storage: GCP storage object.
            service_principal_json_name: Name of the service principal json file
                                         in the GCP storage.
        """
        super().__init__(
            gcp_storage=gcp_storage, 
            service_principal_json_name=service_principal_json_name,
        )

    @staticmethod
    def _get_connector() -> None:
        """Gets GCS connector for spark and copies it in $SPARK_HOME/jars.
        
        The GCS connector is required to read and write data from GCS.
        The connector is downloaded from the official GCS connector repository.
        And then it is copied to the $SPARK_HOME/jars directory.
        """
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
        shutil.move(file_name, directory, copy_function=shutil.copy)

    @staticmethod
    def _authenticate(
        gcp_storage: Storage, 
        service_principal_json_name: str,
    ) -> None:
        """Authenticates to GCP using the service principal json.
        
        The service principal json is downloaded from GCS and is set as an
        environment variable GOOGLE_APPLICATION_CREDENTIALS.

        Args:
            gcp_storage: GCP storage object.
            service_principal_json_name: Name of the service principal json file
                                         in the GCP storage.
        """
        if INITIAL_RUN:
            # Download the service principal json
            gcp_storage.download(
                blob_name=service_principal_json_name,
                file_path=service_principal_json_name,
            )
        # Set the environment variable
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            f"{os.getcwd()}\{service_principal_json_name}"
        )
