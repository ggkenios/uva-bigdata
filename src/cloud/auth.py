from abc import ABC, abstractmethod
import google.auth
from google.auth.credentials import Credentials


class Auth(ABC):
    @abstractmethod
    def get_credentials(cls):
        raise NotImplementedError


class AuthGCP(Auth):
    @classmethod
    def get_credentials(cls) -> Credentials:
        return google.auth.default()[0]
