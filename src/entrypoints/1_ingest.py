from src.cloud import StorageGCP
from src.config import (
    CLOUD_STORAGE,
    DATASET,
    PROJECT_ID,
)


WRITE_PATH = "ingestion"


if __name__ == "__main__":
    # Instantiate the GCP storage
    gcp_storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    # Upload the data from the APIs
    for data in DATASET:
        gcp_storage.upload_from_url(
            blob_name=f"{WRITE_PATH}/{data['name']}.{data['file_type']}",
            url=data["url"],
        )
