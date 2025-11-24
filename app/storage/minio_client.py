from minio import Minio
from starlette.concurrency import run_in_threadpool
from typing import Optional
from io import BytesIO


class MinioWrapper:
    def __init__(
        self, endpoint, access_key: str, secret_key: str, secure: bool = False
    ):
        self._client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    async def ensure_bucket(self, bucket_name: str):
        exists = await run_in_threadpool(self._client.bucket_exists, bucket_name)

        if not exists:
            await run_in_threadpool(self._client.make_bucket, bucket_name)

    async def put_object_from_bytes(
        self,
        bucket_name: str,
        object_name: str,
        data: bytes,
        content_type: Optional[str] = None,
    ):
        final_content_type = content_type or "application/octet-stream"
        await run_in_threadpool(
            self._client.put_object,
            bucket_name,
            object_name,
            BytesIO(data),
            len(data),
            content_type=final_content_type,  # this always expects str
        )

    def presinged_get(self, bucket_name: str, object_name: str):
        return self._client.presigned_get_object(bucket_name, object_name)

    @property
    def client(self):
        return self._client
