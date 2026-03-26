from __future__ import annotations

from collections.abc import Iterable

import boto3
from botocore.client import Config

from fluxdb.config import settings


class MinioStore:
    def __init__(self) -> None:
        self._client = boto3.client(
            "s3",
            endpoint_url=f"{'https' if settings.minio_secure else 'http'}://{settings.minio_endpoint}",
            aws_access_key_id=settings.minio_access_key,
            aws_secret_access_key=settings.minio_secret_key,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
            region_name=settings.minio_region,
        )

    @property
    def client(self):
        return self._client

    def ensure_bucket(self, bucket: str) -> None:
        existing = self._client.list_buckets()
        names = {item["Name"] for item in existing.get("Buckets", [])}
        if bucket not in names:
            self._client.create_bucket(Bucket=bucket)

    def presign_put(self, bucket: str, object_key: str, expires_sec: int = 3600) -> str:
        return self._client.generate_presigned_url(
            "put_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires_sec,
        )

    def presign_get(self, bucket: str, object_key: str, expires_sec: int = 3600) -> str:
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": object_key},
            ExpiresIn=expires_sec,
        )

    def initiate_multipart_upload(self, bucket: str, object_key: str) -> str:
        response = self._client.create_multipart_upload(Bucket=bucket, Key=object_key)
        return str(response["UploadId"])

    def presign_upload_part(self, bucket: str, object_key: str, upload_id: str, part_number: int, expires_sec: int = 3600) -> str:
        return self._client.generate_presigned_url(
            "upload_part",
            Params={
                "Bucket": bucket,
                "Key": object_key,
                "UploadId": upload_id,
                "PartNumber": part_number,
            },
            ExpiresIn=expires_sec,
        )

    def complete_multipart_upload(self, bucket: str, object_key: str, upload_id: str, parts: Iterable[dict]) -> None:
        ordered = sorted(parts, key=lambda item: int(item["PartNumber"]))
        self._client.complete_multipart_upload(
            Bucket=bucket,
            Key=object_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": ordered},
        )

    def abort_multipart_upload(self, bucket: str, object_key: str, upload_id: str) -> None:
        self._client.abort_multipart_upload(Bucket=bucket, Key=object_key, UploadId=upload_id)

    def stat_object(self, bucket: str, object_key: str) -> int:
        metadata = self._client.head_object(Bucket=bucket, Key=object_key)
        return int(metadata.get("ContentLength") or 0)

    def delete_prefix(self, bucket: str, prefix: str) -> int:
        paginator = self._client.get_paginator("list_objects_v2")
        deleted = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if not keys:
                continue
            self._client.delete_objects(Bucket=bucket, Delete={"Objects": keys, "Quiet": True})
            deleted += len(keys)
        return deleted


minio_store = MinioStore()
