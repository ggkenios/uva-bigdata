from src.cloud import SparkGCP, StorageGCP
from src.config import (
    PROJECT_ID,
    CLOUD_STORAGE,
    SERVICE_PRINCIPAL_JSON,
)


READ_PATH = "datalake/silver"
WRITE_PATH = "datalake/gold"


def main():
    # Set the spark configuration
    gcp_storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    spark = SparkGCP(
        gcp_storage=gcp_storage,
        service_principal_json_name=SERVICE_PRINCIPAL_JSON,
    ).spark

    df = spark.read.parquet(f"gs://{CLOUD_STORAGE}/{READ_PATH}")
    df = (
        df
        .filter("country == 'EU27_2020'")
        .filter("sex == 'T'")
    )
    c = df.count()
    print(c)
    df.show()


if __name__ == "__main__":
    main()
