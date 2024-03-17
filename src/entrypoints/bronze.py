from src.cloud.spark_session import SparkGCP
from src.cloud.storage import StorageGCP
from src.config import (
    PROJECT_ID,
    CLOUD_STORAGE,
    SERVICE_PRINCIPAL_JSON,
)


READ_PATH = "ingestion/estat_hlth_cd_asdr2.tsv"

# Set the spark configuration
gcp_storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
spark = SparkGCP(
    gcp_storage=gcp_storage,
    service_principal_json_name=SERVICE_PRINCIPAL_JSON,
).spark

df = spark.read.csv(f"gs://{CLOUD_STORAGE}/{READ_PATH}", sep=r'\t', header=True)
df.show(5)
