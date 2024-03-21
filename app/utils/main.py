import pandas as pd


def read_parquet_from_gcs(bucket_name: str, file_name: str) -> pd.DataFrame:
    """Reads a parquet file from Google Cloud Storage."""
    return pd.read_parquet(f"gs://{bucket_name}/{file_name}")
