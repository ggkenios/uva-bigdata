from src.cloud.spark_session import SparkGCP
from src.cloud.storage import StorageGCP
from src.config import (
    PROJECT_ID,
    CLOUD_STORAGE,
    SERVICE_PRINCIPAL_JSON,
    DATASET,
)

# Paths
READ_PATH = "ingestion"
WRITE_PATH = "datalake/bronze"


if __name__ == "__main__":
    # Set the spark configuration
    gcp_storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    spark = SparkGCP(
        gcp_storage=gcp_storage,
        service_principal_json_name=SERVICE_PRINCIPAL_JSON,
    ).spark

    for data in DATASET:
        file_name = f"{data['name']}.{data['file_type']}"
        # Read data from the ingestion layer
        df = spark.read.csv(
            path=f"gs://{CLOUD_STORAGE}/{READ_PATH}/{file_name}",
            inferSchema=True,
            header=True,
        )
        # Write data to the bronze layer
        df.write.parquet(
            path=f"gs://{CLOUD_STORAGE}/{WRITE_PATH}/{data['name']}",
            mode="overwrite",
        )
