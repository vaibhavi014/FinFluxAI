from __future__ import annotations

from pathlib import Path

from finflux.config import Settings
from finflux.exceptions import TransientProcessingError


class ObjectStore:
    def put(self, zone: str, key: str, json_body: str) -> None:
        raise NotImplementedError


class LocalObjectStore(ObjectStore):
    def __init__(self, root: str):
        self.root = Path(root)

    def put(self, zone: str, key: str, json_body: str) -> None:
        path = self.root / zone / key
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json_body, encoding="utf-8")
        except OSError as ex:
            raise TransientProcessingError("STORAGE_FAILURE", f"Could not write {path}") from ex


class S3ObjectStore(ObjectStore):
    def __init__(self, settings: Settings):
        if not settings.s3_bucket:
            raise RuntimeError("AWS_S3_BUCKET must be set when STORAGE_TYPE=s3")
        import boto3

        self.bucket = settings.s3_bucket
        self.client = boto3.client("s3", region_name=settings.s3_region)

    def put(self, zone: str, key: str, json_body: str) -> None:
        object_key = f"{zone}/{key.replace(chr(92), '/')}"
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=json_body.encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as ex:
            raise TransientProcessingError("STORAGE_FAILURE", f"Could not write S3 key {object_key}") from ex


def build_store(settings: Settings) -> ObjectStore:
    if settings.storage_type == "s3":
        return S3ObjectStore(settings)
    return LocalObjectStore(settings.storage_path)
