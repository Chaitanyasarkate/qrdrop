import logging
from typing import Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from config import settings

logger = logging.getLogger("qrdrop.storage")

_s3_client = None


def get_s3_client():
    """Returns a singleton boto3 S3 client configured with s3v4 signature for MinIO."""
    global _s3_client
    if _s3_client is None:
        logger.info(
            "Configuring S3 client with endpoint '%s' (region: '%s', s3v4)...",
            settings.MINIO_ENDPOINT,
            settings.MINIO_REGION,
        )
        _s3_client = boto3.client(
            "s3",
            endpoint_url=settings.MINIO_ENDPOINT,
            aws_access_key_id=settings.MINIO_ROOT_USER,
            aws_secret_access_key=settings.MINIO_ROOT_PASSWORD,
            region_name=settings.MINIO_REGION,
            use_ssl=settings.is_s3_ssl,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
            ),
        )
    return _s3_client


def init_storage() -> None:
    """Verifies that the target MinIO / S3 bucket exists; creates it if missing."""
    client = get_s3_client()
    bucket_name = settings.MINIO_BUCKET
    logger.info("Verifying MinIO/S3 storage bucket '%s'...", bucket_name)
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("MinIO/S3 bucket '%s' verified and accessible.", bucket_name)
    except ClientError as e:
        error_code = str(e.response.get("Error", {}).get("Code", ""))
        # 404 Not Found or NoSuchBucket means the bucket needs creation
        if error_code in ("404", "NoSuchBucket"):
            logger.info("Bucket '%s' does not exist. Attempting creation...", bucket_name)
            try:
                if settings.MINIO_REGION in ("us-east-1", "auto", ""):
                    client.create_bucket(Bucket=bucket_name)
                else:
                    client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": settings.MINIO_REGION},
                    )
                logger.info("Bucket '%s' successfully created.", bucket_name)
            except Exception as create_err:
                logger.warning("Bucket auto-creation for '%s': %s (bucket may be managed externally)", bucket_name, create_err)
        else:
            logger.warning("Storage bucket verification notice for '%s': %s", bucket_name, e)


def check_storage_health() -> bool:
    """Checks whether the configured bucket is responsive."""
    try:
        client = get_s3_client()
        client.head_bucket(Bucket=settings.MINIO_BUCKET)
        return True
    except Exception as e:
        logger.error("Storage health check failed: %s", e)
        return False


from dataclasses import dataclass
from typing import AsyncIterator, List, Dict, Any
import asyncio
import hashlib

MIN_PART_SIZE = 5 * 1024 * 1024  # 5 MiB (S3 minimum part size for multipart uploads)


@dataclass
class StreamUploadResult:
    storage_key: str
    file_size_bytes: int
    sha256_hash: str
    mime_type: str


async def upload_stream_to_s3(
    byte_stream: AsyncIterator[bytes],
    storage_key: str,
    mime_type: str = "application/octet-stream",
    bucket: Optional[str] = None,
) -> StreamUploadResult:
    """
    Streams incoming byte chunks into MinIO using S3 Multipart Upload API.
    Guarantees flat memory consumption (<60MB) by accumulating only up to 5MB chunks.
    Simultaneously computes SHA-256 on the fly and tracks byte count.
    Executes all boto3 I/O operations via asyncio.to_thread to keep the event loop non-blocking.
    Automatically aborts multipart upload on any error.
    """
    client = get_s3_client()
    target_bucket = bucket or settings.MINIO_BUCKET

    hasher = hashlib.sha256()
    total_bytes = 0
    buffer = bytearray()
    parts: List[Dict[str, Any]] = []
    part_number = 1

    # Initialize S3 Multipart Upload
    create_resp = await asyncio.to_thread(
        client.create_multipart_upload,
        Bucket=target_bucket,
        Key=storage_key,
        ContentType=mime_type,
    )
    upload_id = create_resp["UploadId"]
    logger.info("Initiated S3 multipart upload for '%s' (UploadId: %s)", storage_key, upload_id)

    try:
        async for chunk in byte_stream:
            if not chunk:
                continue
            hasher.update(chunk)
            total_bytes += len(chunk)
            buffer.extend(chunk)

            # Upload completed 5MB parts
            while len(buffer) >= MIN_PART_SIZE:
                part_data = bytes(buffer[:MIN_PART_SIZE])
                del buffer[:MIN_PART_SIZE]

                part_resp = await asyncio.to_thread(
                    client.upload_part,
                    Bucket=target_bucket,
                    Key=storage_key,
                    UploadId=upload_id,
                    PartNumber=part_number,
                    Body=part_data,
                )
                parts.append({"PartNumber": part_number, "ETag": part_resp["ETag"]})
                logger.debug(
                    "Uploaded part %d (%d bytes) for '%s'",
                    part_number,
                    len(part_data),
                    storage_key,
                )
                part_number += 1

        # Upload final part (remaining bytes, or part 1 if file < 5MB or 0-byte file)
        if len(buffer) > 0 or part_number == 1:
            part_data = bytes(buffer)
            buffer.clear()
            part_resp = await asyncio.to_thread(
                client.upload_part,
                Bucket=target_bucket,
                Key=storage_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=part_data,
            )
            parts.append({"PartNumber": part_number, "ETag": part_resp["ETag"]})
            logger.debug(
                "Uploaded final part %d (%d bytes) for '%s'",
                part_number,
                len(part_data),
                storage_key,
            )

        # Finalize and complete multipart upload
        await asyncio.to_thread(
            client.complete_multipart_upload,
            Bucket=target_bucket,
            Key=storage_key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        logger.info(
            "Successfully completed multipart upload for '%s': %d bytes in %d part(s)",
            storage_key,
            total_bytes,
            len(parts),
        )

    except Exception as exc:
        logger.error(
            "Multipart upload failed for '%s' (UploadId: %s), aborting: %s",
            storage_key,
            upload_id,
            exc,
        )
        try:
            await asyncio.to_thread(
                client.abort_multipart_upload,
                Bucket=target_bucket,
                Key=storage_key,
                UploadId=upload_id,
            )
            logger.info("Successfully aborted multipart upload for '%s'", storage_key)
        except Exception as abort_err:
            logger.error("Failed to abort multipart upload for '%s': %s", storage_key, abort_err)
        raise

    return StreamUploadResult(
        storage_key=storage_key,
        file_size_bytes=total_bytes,
        sha256_hash=hasher.hexdigest(),
        mime_type=mime_type,
    )


async def delete_s3_object(storage_key: str, bucket: Optional[str] = None) -> bool:
    """Deletes an object from MinIO."""
    client = get_s3_client()
    target_bucket = bucket or settings.MINIO_BUCKET
    try:
        await asyncio.to_thread(
            client.delete_object,
            Bucket=target_bucket,
            Key=storage_key,
        )
        logger.info("Deleted object '%s' from bucket '%s'", storage_key, target_bucket)
        return True
    except Exception as e:
        logger.error("Failed to delete object '%s' from MinIO: %s", storage_key, e)
        return False


# In-flight stream tracker to solve the final-download deletion race condition
import collections

_active_stream_counts: collections.defaultdict = collections.defaultdict(int)
_stream_lock = asyncio.Lock()


async def register_stream_start(storage_key: str) -> None:
    """Increments the in-flight reader counter for a storage key."""
    async with _stream_lock:
        _active_stream_counts[storage_key] += 1


async def register_stream_end(storage_key: str) -> int:
    """Decrements the in-flight reader counter and returns remaining count."""
    async with _stream_lock:
        _active_stream_counts[storage_key] = max(0, _active_stream_counts[storage_key] - 1)
        remaining = _active_stream_counts[storage_key]
        if remaining == 0 and storage_key in _active_stream_counts:
            del _active_stream_counts[storage_key]
        return remaining


def is_storage_key_in_use(storage_key: str) -> bool:
    """Checks whether any active downloads are currently reading this object."""
    return _active_stream_counts.get(storage_key, 0) > 0


async def get_s3_object_stream(
    storage_key: str,
    byte_range: Optional[str] = None,
    chunk_size: int = 65536,
    on_finish=None,
):
    """
    Streams an S3 object in chunks via asyncio.to_thread without blocking the event loop.
    Supports HTTP Byte-Range requests.
    Tracks in-flight readers to protect against deletion during final download.
    Executes on_finish(remaining_readers) in the finally block.
    """
    client = get_s3_client()
    kwargs = {"Bucket": settings.MINIO_BUCKET, "Key": storage_key}
    if byte_range:
        kwargs["Range"] = byte_range

    response = await asyncio.to_thread(client.get_object, **kwargs)
    body = response["Body"]

    await register_stream_start(storage_key)
    try:
        while True:
            chunk = await asyncio.to_thread(body.read, chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        await asyncio.to_thread(body.close)
        remaining_readers = await register_stream_end(storage_key)
        if on_finish is not None:
            try:
                await on_finish(remaining_readers)
            except Exception as cb_err:
                logger.error("Error in stream completion callback for '%s': %s", storage_key, cb_err)


