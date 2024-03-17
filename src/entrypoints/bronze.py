from src.config import CLOUD_STORAGE, DATASET
from src.spark_init import ConfigGCP


READ_PATH = "ingestion"
WRITE_PATH = "datalake/bronze"


def main():
    # Set the spark configuration
    spark = ConfigGCP.spark

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

if __name__ == "__main__":
    main()
