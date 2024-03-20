from src.config import CLOUD_STORAGE
from src.spark_init import ConfigGCP

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

READ_PATH = "datalake/silver"
WRITE_PATH = "datalake/gold"


def print_count(df: DataFrame) -> None:
    print(df.count())


def main():
    # Set the spark configuration
    spark = ConfigGCP.spark

    #df = spark.read.parquet(f"gs://{CLOUD_STORAGE}/{READ_PATH}")
    # df = (
    #     df
    #     .filter("country == 'EU27_2020'")
    #     .filter("sex == 'T'")
    # )
    tables = ["joined"] 
    for table in tables:
        df = spark.read.parquet(f"gs://{CLOUD_STORAGE}/{READ_PATH}/{table}")
        print_count(df)
        df.show()

    from pyspark.ml.stat import Correlation

    pearsonCorr  = Correlation.corr(df.dropna(), "pay_gap_rate", "pearson").collect()[0][0]
    print(str(pearsonCorr).replace('nan', 'NaN'))
    # df_count = (
    #     df
    #     .groupBy("country")
    #     .count()
    # )
    # max = df_count.agg({"count": "max"}).collect()[0][0]

    # df_countries = (
    #     df_count
    #     .filter(F.col("count") == max)
    #     .select("country")
    #     .collect()
    # )
    # countries = [row.country for row in df_countries]
    # for i in countries:
    #     print(i)
    
    #df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()


if __name__ == "__main__":
    main()
