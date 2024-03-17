from abc import ABC, abstractmethod
from google.cloud import storage
from io import BytesIO
import requests


class Storage(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def download(self, blob_name: str, file_path: str) -> None:
        """Downloads a file from the storage to the local file system."""
        raise NotImplementedError

    @abstractmethod
    def upload(self, *args, **kwargs) -> None:
        """Uploads a file from the local file system to the storage."""
        raise NotImplementedError
    
    @abstractmethod
    def upload_from_url(self, *args, **kwargs) -> None:
        """Uploads a file from a URL to the storage."""
        raise NotImplementedError


class StorageGCP(Storage):
    def __init__(self, project_id: str, bucket_name: str):
        """
        Args:
            project_id: GCP project ID.
            bucket_name: The bucket name of GCS.
        """
        self.bucket_name = bucket_name
        self.bucket = storage.Client(project_id).bucket(bucket_name)

    def download(self, blob_name: str, file_path: str) -> None:
        """Downloads a file from GCS to the local file system.
        
        Args:
            blob_name: The name of the blob in the bucket.
            file_path: The path to save the file in the local file system.
        """
        self.bucket.blob(blob_name).download_to_filename(file_path)

    def upload(self, blob_name: str, file_path: str) -> None:
        """Uploads a file from the local file system to GCS.
        
        Args:
            blob_name: The name of the blob in the bucket.
            file_path: The path of the file in the local file system.
        """
        self.bucket.blob(blob_name).upload_from_filename(file_path)
        print(f"File uploaded: 'gs://{self.bucket_name}/{blob_name}'")
    
    def upload_from_url(self, blob_name: str, url: str) -> None:
        """Uploads a file from a URL to GCS.
        
        Args:
            blob_name: The name of the blob in the bucket.
            url: The URL of the file to be uploaded.
        """
        # Fetch the file from the URL
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to fetch the file from URL.")
        bytes_io = BytesIO(response.content)
        # Upload the file to the bucket
        self.bucket.blob(blob_name).upload_from_file(bytes_io)
        print(f"File uploaded: 'gs://{self.bucket_name}/{blob_name}'")
