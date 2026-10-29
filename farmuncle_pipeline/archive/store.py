"""Thin R2 (S3-compatible) client. Credentials come from the environment, never from code."""

from __future__ import annotations

import os


class R2Store:
    def __init__(self, client, bucket: str) -> None:
        self._client = client
        self.bucket = bucket

    # MANDI_R2_* is the separate Cloudflare account for mandi data. The plain R2_* secrets belong to the
    # old account and are deliberately never read here.
    _REQUIRED = ("MANDI_R2_ACCOUNT_ID", "MANDI_R2_ACCESS_KEY_ID", "MANDI_R2_SECRET_ACCESS_KEY")

    @classmethod
    def configured(cls) -> bool:
        return all(os.environ.get(name) for name in cls._REQUIRED)

    @classmethod
    def from_env(cls) -> "R2Store":
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            endpoint_url=f"https://{os.environ['MANDI_R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["MANDI_R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["MANDI_R2_SECRET_ACCESS_KEY"],
            region_name="auto",
            config=Config(max_pool_connections=64),  # the publishers write from many threads
        )
        return cls(client, os.environ.get("MANDI_R2_BUCKET") or "agrouncle-mandi")

    def ensure_bucket(self) -> None:
        from botocore.exceptions import ClientError

        try:
            self._client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in ("404", "NoSuchBucket", "NotFound"):
                raise
            self._client.create_bucket(Bucket=self.bucket)

    def put(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
        self._client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    def get(self, key: str) -> bytes:
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list(self, prefix: str) -> list[str]:
        keys, token = [], None
        while True:
            page = self._client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, **({"ContinuationToken": token} if token else {}))
            keys += [o["Key"] for o in page.get("Contents", [])]
            token = page.get("NextContinuationToken")
            if not token:
                return keys

    def size(self, key: str) -> int:
        return int(self._client.head_object(Bucket=self.bucket, Key=key)["ContentLength"])

    def selfcheck(self) -> None:
        """Write and read back a small file, so a bad token, bucket or endpoint fails loudly before any data moves."""
        import datetime

        body = f"ok {datetime.datetime.now(datetime.timezone.utc).isoformat()}".encode()
        self.put("health/last_check.txt", body, "text/plain")
        if self.size("health/last_check.txt") != len(body):
            raise RuntimeError("R2 self-check failed: stored size does not match")
