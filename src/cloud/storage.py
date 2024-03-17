from abc import ABC, abstractmethod
from google.cloud import storage


class Storage(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def upload(self, *args,) -> None:
        raise NotImplementedError

    @abstractmethod
    def download(self, blob_name: str, file_path: str) -> None:
        raise NotImplementedError


class StorageGCP(Storage):
    def __init__(self, project_id: str, bucket_name: str):
        self.bucket_name = bucket_name
        self.bucket = storage.Client(project_id).get_bucket(bucket_name)

    def upload(self, blob_name: str, file_path: str) -> None:
        blob = self.bucket.blob(blob_name)
        blob.upload_from_filename(file_path)

    def download(self, blob_name: str, file_path: str) -> None:
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(file_path)
