from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import CLOUD_STORAGE, DATASET
from src.config.silver import FILTER, DROP_COLS, RENAME_COLS, JOINS
from src.spark_init import SparkInit


READ_PATH = "datalake/bronze"
WRITE_PATH = "datalake/silver/unemployment_crime_pay_gap"


def main():
    # Set the spark configuration
    spark = SparkInit.spark

    dfs: dict[str, DataFrame] = {}
    for data in DATASET:
        table = data["name"]
        # Read data from the bronze layer
        dfs[table] = spark.read.parquet(f"gs://{CLOUD_STORAGE}/{READ_PATH}/{table}")
 
        # Filter the data
        for filtering in FILTER[table]:
            dfs[table] = dfs[table].filter(filtering)

        # Drop columns
        dfs[table] = dfs[table].drop(*DROP_COLS[table])

        # Rename columns
        for old, new in RENAME_COLS[table].items():
            dfs[table] = dfs[table].withColumnRenamed(old, new)

    # Perform joins
    for join in JOINS:
        dfs[join["left"]] = dfs[join["left"]].join(
            other=F.broadcast(dfs[join["right"]]),
            on=join["on"],
            how=join["how"],
        )

    # Select distinct rows
    df = (
        dfs[join["left"]]
        .distinct()
    )

    # Write data to the silver layer
    df.write.parquet(
        path=f"gs://{CLOUD_STORAGE}/{WRITE_PATH}",
        mode="overwrite",
    )

    print(df.count())


if __name__ == "__main__":
    main()
