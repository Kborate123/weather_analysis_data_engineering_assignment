from typing import TypeVar
from dagster import ConfigurableResource
from pydantic import Field
from processing_weather.utils import build_url

T = TypeVar("T")


class DBConfigMissingError(Exception):
    pass


class S3BucketResource(ConfigurableResource):
    """A S3 bucket resource to store processed crawled data moved from mongodb."""

    endpoint_url: str = Field(description="Specifies an S3 endpoint URL")
    access_key: str = Field(description="Access key")
    secret_key: str = Field(description="Secret key")
    bucket_name: str = Field(description="Bucket name")
    path_prefix: str = Field(description="Path prefix")

    @property
    def storage_options(self):
        return {
            "endpoint_url": self.endpoint_url,
            "key": self.access_key,
            "secret": self.secret_key,
        }

    def build_uri(self, key: str | list[str], protocol: str = "s3a") -> str:
        """
        Build URI of the given key

        Args:
            key (str | list[str]): key or key parts
            protocol (str, optional): URI protocol. Defaults to "s3a".

        Returns:
            str: Complete S3 bucket URI of the key
        """
        parts = [self.path_prefix] if self.path_prefix else []
        if isinstance(key, list):
            parts += key
        else:
            parts.append(key)
        return build_url(protocol, self.bucket_name, *parts)
