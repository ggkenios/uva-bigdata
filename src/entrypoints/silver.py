from src.cloud import SparkGCP, StorageGCP
from config.config import (
    PROJECT_ID,
    CLOUD_STORAGE,
    SERVICE_PRINCIPAL_JSON,
    DATASET,
)
from src.config.silver import (
    FILTER,
    DROP_COLS,
    RENAME_COLS,
    JOINS,
)


READ_PATH = "datalake/bronze"
WRITE_PATH = "datalake/silver"


def main():
    # Set the spark configuration
    gcp_storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    spark = SparkGCP(
        gcp_storage=gcp_storage,
        service_principal_json_name=SERVICE_PRINCIPAL_JSON,
    ).spark

    dfs = {}
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
            other=dfs[join["right"]],
            on=join["on"],
            how=join["how"],
        )

    df = (
        dfs[join["left"]]
        .distinct()
        .na.drop()
    )

    # Write data to the silver layer
    df.write.parquet(
        path=f"gs://{CLOUD_STORAGE}/{WRITE_PATH}",
        mode="overwrite",
    )

if __name__ == "__main__":
    main()
