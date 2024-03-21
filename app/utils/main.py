import pandas as pd
import streamlit as st
from io import BytesIO
import requests


def read_parquet_from_gcs(bucket_name: str, file_name: str) -> pd.DataFrame:
    """Reads a parquet file from Google Cloud Storage."""
    return pd.read_parquet(f"gs://{bucket_name}/{file_name}")


@st.cache_data
def get_image(url: str) -> BytesIO:
    r = requests.get(url)
    return BytesIO(r.content)