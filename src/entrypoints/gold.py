from src.config import CLOUD_STORAGE
from src.spark_init import ConfigGCP


READ_PATH = "datalake/silver"
WRITE_PATH = "datalake/gold"


def main():
    # Set the spark configuration
    spark = ConfigGCP.spark

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
