from src.config import CLOUD_STORAGE
from app.utils import read_parquet_from_gcs


df = read_parquet_from_gcs(
    bucket_name=CLOUD_STORAGE, 
    file_name="datalake/gold/unemployment_crime_pay_gap"
)
