from abc import ABC, abstractmethod
from google.cloud import secretmanager
import json


class Secret(ABC):
    @abstractmethod
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def get_secret(self, secret_name: str) -> str:
        raise NotImplementedError


class SecretGCP(Secret):
    def __init__(self, project_id: str):
        self.project_id = project_id

    def get_secret(self, secret_name: str) -> str:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{self.project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")

    def get_service_account_secret(self, secret_name: str) -> str:
        secret = self.get_secret(secret_name)
        return json.loads(secret)
