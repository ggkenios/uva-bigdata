from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config import CLOUD_STORAGE
from src.config.gold import FILTER, DROP_COLS
from src.spark_init import SparkInit


READ_PATH = "datalake/silver/unemployment_crime_pay_gap"
WRITE_PATH = "datalake/gold/unemployment_crime_pay_gap"


def main():
    # Set the spark configuration
    spark = SparkInit.spark

    # Read data from the silver layer
    df = spark.read.parquet(f"gs://{CLOUD_STORAGE}/{READ_PATH}")

    # Filter the data (Aggregations)
    for filtering in FILTER:
        df = df.filter(filtering)

    # Drop columns
    df = df.drop(*DROP_COLS)

    # Remove NA values
    df = df.dropna()

    # Cache the group by country count
    df_country_count = (
        df
        .groupBy("country")
        .count()
        .cache()
    )

    # Find max count per country
    max = df_country_count.agg({"count": "max"}).collect()[0][0]

    # Find the countries with the max count
    df_countries = (
        df_country_count
        .filter(F.col("count") == max)
        .select("country")
        .collect()
    )

    countries_with_most_records = [row.country for row in df_countries]
    df = df.filter(F.col("country").isin(countries_with_most_records))

    # Find the number of unique years
    df.write.parquet(
        path=f"gs://{CLOUD_STORAGE}/{WRITE_PATH}",
        mode="overwrite",
    )


if __name__ == "__main__":
    main()
