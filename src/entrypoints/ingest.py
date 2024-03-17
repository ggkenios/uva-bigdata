from src.cloud import StorageGCP
from src.config import CLOUD_STORAGE, DATASET, PROJECT_ID


WRITE_PATH = "ingestion"


def main():
    # Instantiate the GCP storage
    storage = StorageGCP(PROJECT_ID, CLOUD_STORAGE)
    # Upload the data from the APIs
    for data in DATASET:
        storage.upload_from_url(
            blob_name=f"{WRITE_PATH}/{data['name']}.{data['file_type']}",
            url=data["url"],
        )

if __name__ == "__main__":
    main()
