import os
import uuid
import mimetypes
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, UploadFile, File, Form, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from config import settings
from database import (
    init_db_pool,
    close_db_pool,
    check_db_health,
    generate_unique_pin,
    create_drop_record,
    get_drop_by_pin,
    atomic_decrement_and_claim_drop,
    mark_drop_purged,
    get_system_metrics,
)
from storage import (
    init_storage,
    check_storage_health,
    upload_stream_to_s3,
    delete_s3_object,
    get_s3_object_stream,
    is_storage_key_in_use,
)
from qr import generate_svg_qr, format_cli_summary, format_size
from janitor import storage_janitor_worker, send_discord_alert

logger = logging.getLogger("qrdrop.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI Lifespan Context Manager managing database pool, MinIO storage, and Storage Janitor."""
    logger.info("Starting up QR-Drop Engine...")

    # Initialize PostgreSQL connection pool
    try:
        await init_db_pool()
    except Exception as e:
        logger.error("Failed to initialize database pool during startup: %s", e)
        raise

    # Initialize MinIO / S3 bucket verification
    try:
        init_storage()
    except Exception as e:
        logger.error("Failed to verify/create MinIO storage bucket during startup: %s", e)
        raise

    # Start the background Storage Janitor worker
    janitor_task = asyncio.create_task(storage_janitor_worker())

    logger.info("QR-Drop Engine started successfully with Storage Janitor active.")
    yield

    # Clean shutdown
    logger.info("Shutting down QR-Drop Engine...")
    janitor_task.cancel()
    await asyncio.gather(janitor_task, return_exceptions=True)

    await close_db_pool()
    logger.info("QR-Drop Engine shutdown complete.")


app = FastAPI(
    title="QR-Drop API",
    description="Ephemeral Streaming File-Transfer & S3 Object Storage Engine",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static file mount
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def is_cli_client(request: Request) -> bool:
    """Detects whether the incoming request originates from a terminal tool (curl, wget, PowerShell)."""
    ua = request.headers.get("user-agent", "").lower()
    accept = request.headers.get("accept", "").lower()
    if any(cli in ua for cli in ("curl", "wget", "powershell")):
        return True
    if "text/plain" in accept and "application/json" not in accept:
        return True
    return False


def parse_range_header(range_header: str, total_size: int) -> Optional[Tuple[int, int]]:
    """
    Parses a single HTTP Byte-Range header (e.g. 'bytes=0-499' or 'bytes=500-' or 'bytes=-500').
    Returns (start, end) tuple of inclusive byte offsets, or None if invalid.
    """
    if not range_header or not range_header.startswith("bytes="):
        return None

    try:
        range_val = range_header[len("bytes="):].strip()
        if "," in range_val:
            # Multipart ranges not supported, fallback to full stream
            return None

        parts = range_val.split("-")
        if len(parts) != 2:
            return None

        start_str, end_str = parts[0].strip(), parts[1].strip()

        if start_str and end_str:
            start = int(start_str)
            end = int(end_str)
        elif start_str:
            start = int(start_str)
            end = total_size - 1
        elif end_str:
            suffix_len = int(end_str)
            start = max(0, total_size - suffix_len)
            end = total_size - 1
        else:
            return None

        if 0 <= start <= end < total_size:
            return (start, end)
        return None
    except Exception:
        return None


@app.get("/", include_in_schema=False)
async def root():
    """Serves the landing page or metadata summary."""
    index_path = os.path.join(STATIC_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse(
        content={
            "app": "QR-Drop API",
            "version": "1.0.0",
            "message": "Welcome to QR-Drop Engine. Visit /health or /docs.",
        }
    )


@app.get(
    "/health",
    summary="Infrastructure Healthcheck",
    description="Checks the operational status of the API, PostgreSQL pool, and MinIO S3 storage.",
)
async def health_check():
    """Healthcheck endpoint verifying DB and MinIO connectivity."""
    db_healthy = await check_db_health()
    storage_healthy = check_storage_health()

    all_healthy = db_healthy and storage_healthy
    http_status = (
        status.HTTP_200_OK if all_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    )

    return JSONResponse(
        status_code=http_status,
        content={
            "status": "healthy" if all_healthy else "degraded",
            "database": db_healthy,
            "storage": storage_healthy,
            "environment": settings.ENVIRONMENT,
            "public_url": settings.PUBLIC_BASE_URL,
            "version": "1.0.0",
        },
    )


@app.post(
    "/api/upload",
    summary="Browser Multipart Streaming Upload",
    description=(
        "Streams multipart file upload in 64KB chunks directly into MinIO via S3 Multipart Upload API. "
        "Computes SHA-256 on the fly without accumulating the whole file in RAM. "
        "Returns JSON metadata including an inline SVG QR code."
    ),
)
async def upload_multipart(
    request: Request,
    file: UploadFile = File(...),
    max_downloads: Optional[int] = Form(None),
    expiry_minutes: Optional[int] = Form(None),
):
    """Multipart browser upload endpoint with zero-RAM streaming."""
    # Read optional form values or fallback to custom headers
    if max_downloads is None:
        header_val = request.headers.get("X-Max-Downloads")
        try:
            max_downloads = int(header_val) if header_val else 3
        except ValueError:
            max_downloads = 3

    if expiry_minutes is None:
        header_val = request.headers.get("X-Expiry-Minutes")
        try:
            expiry_minutes = int(header_val) if header_val else 60
        except ValueError:
            expiry_minutes = 60

    max_downloads = max(1, min(max_downloads, 100))
    expiry_minutes = max(1, min(expiry_minutes, 10080))

    clean_filename = os.path.basename(file.filename or f"drop_{uuid.uuid4().hex[:6]}.bin")

    # 1. Generate unique 6-digit PIN
    pin = await generate_unique_pin()

    # 2. Derive unique storage key and mime type
    storage_key = f"{pin}_{uuid.uuid4().hex[:8]}_{clean_filename}"
    mime_type = file.content_type
    if not mime_type or mime_type in ("application/octet-stream", "application/x-www-form-urlencoded"):
        guessed, _ = mimetypes.guess_type(clean_filename)
        mime_type = guessed or "application/octet-stream"
    else:
        mime_type = mime_type.split(";")[0].strip()

    # 3. Stream in 64KB chunks to S3 multipart uploader without buffering full file
    async def file_chunk_generator():
        while True:
            chunk = await file.read(65536)
            if not chunk:
                break
            yield chunk

    try:
        upload_result = await upload_stream_to_s3(
            byte_stream=file_chunk_generator(),
            storage_key=storage_key,
            mime_type=mime_type,
        )
    except Exception as e:
        logger.error("Multipart streaming upload failed for '%s': %s", clean_filename, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Streaming upload failed: {str(e)}",
        )

    # 4. Insert drop metadata into PostgreSQL
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)
    try:
        await create_drop_record(
            pin=pin,
            original_name=clean_filename,
            storage_key=storage_key,
            file_size_bytes=upload_result.file_size_bytes,
            mime_type=mime_type,
            sha256_hash=upload_result.sha256_hash,
            max_downloads=max_downloads,
            expires_at=expires_at,
        )
    except Exception as db_err:
        logger.error("Failed to insert drop record into database: %s", db_err)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record drop metadata in database.",
        )

    download_url = f"{settings.PUBLIC_BASE_URL}/d/{pin}"
    svg_qr = generate_svg_qr(download_url)

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "pin": pin,
            "download_url": download_url,
            "original_name": clean_filename,
            "file_size_bytes": upload_result.file_size_bytes,
            "sha256_hash": upload_result.sha256_hash,
            "expires_at": expires_at.isoformat(),
            "max_downloads": max_downloads,
            "downloads_remaining": max_downloads,
            "qr_code_svg": svg_qr,
        },
    )


@app.put(
    "/{filename}",
    summary="CLI Raw Streaming Upload",
    description=(
        "Raw byte streaming upload designed for `curl -T filename`. "
        "Streams incoming bytes directly into MinIO via S3 Multipart Upload API while calculating SHA-256 on the fly. "
        "Returns clean ASCII summary with terminal ASCII QR code for CLI agents, or JSON for standard HTTP clients."
    ),
)
async def upload_cli_stream(filename: str, request: Request):
    """Raw CLI stream upload endpoint for curl -T."""
    clean_filename = os.path.basename(filename.strip())
    reserved_routes = {"health", "docs", "openapi.json", "redoc", "favicon.ico", "api", "static"}
    if not clean_filename or clean_filename in reserved_routes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid filename specified for streaming upload.",
        )

    # Parse optional headers: X-Max-Downloads (default 3), X-Expiry-Minutes (default 60)
    try:
        max_downloads = int(request.headers.get("X-Max-Downloads", 3))
    except (ValueError, TypeError):
        max_downloads = 3

    try:
        expiry_minutes = int(request.headers.get("X-Expiry-Minutes", 60))
    except (ValueError, TypeError):
        expiry_minutes = 60

    max_downloads = max(1, min(max_downloads, 100))
    expiry_minutes = max(1, min(expiry_minutes, 10080))  # Max 7 days

    # 1. Generate unique 6-digit PIN
    pin = await generate_unique_pin()

    # 2. Derive unique storage key and mime type
    storage_key = f"{pin}_{uuid.uuid4().hex[:8]}_{clean_filename}"
    content_type = request.headers.get("content-type")
    if not content_type or content_type in ("application/octet-stream", "application/x-www-form-urlencoded"):
        guessed, _ = mimetypes.guess_type(clean_filename)
        mime_type = guessed or "application/octet-stream"
    else:
        mime_type = content_type.split(";")[0].strip()

    # 3. Stream bytes to MinIO using non-buffering multipart upload
    try:
        upload_result = await upload_stream_to_s3(
            byte_stream=request.stream(),
            storage_key=storage_key,
            mime_type=mime_type,
        )
    except Exception as e:
        logger.error("CLI streaming upload failed for '%s': %s", clean_filename, e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Streaming upload failed: {str(e)}",
        )

    # 4. Insert drop metadata into PostgreSQL
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=expiry_minutes)
    try:
        await create_drop_record(
            pin=pin,
            original_name=clean_filename,
            storage_key=storage_key,
            file_size_bytes=upload_result.file_size_bytes,
            mime_type=mime_type,
            sha256_hash=upload_result.sha256_hash,
            max_downloads=max_downloads,
            expires_at=expires_at,
        )
    except Exception as db_err:
        logger.error("Failed to persist drop record in database: %s", db_err)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record drop metadata in database.",
        )

    download_url = f"{settings.PUBLIC_BASE_URL}/d/{pin}"

    # 5. Dual Response: ASCII box + terminal ASCII QR code for CLI clients; JSON for other clients
    if is_cli_client(request):
        cli_output = format_cli_summary(
            original_name=clean_filename,
            file_size_bytes=upload_result.file_size_bytes,
            sha256_hash=upload_result.sha256_hash,
            expires_at=expires_at,
            max_downloads=max_downloads,
            download_url=download_url,
            pin=pin,
        )
        return PlainTextResponse(content=cli_output, status_code=status.HTTP_201_CREATED)

    return JSONResponse(
        status_code=status.HTTP_201_CREATED,
        content={
            "pin": pin,
            "download_url": download_url,
            "original_name": clean_filename,
            "file_size_bytes": upload_result.file_size_bytes,
            "sha256_hash": upload_result.sha256_hash,
            "expires_at": expires_at.isoformat(),
            "max_downloads": max_downloads,
            "downloads_remaining": max_downloads,
        },
    )


@app.head(
    "/d/{pin}",
    summary="Inspect Drop Headers",
    description="Returns metadata headers (Content-Length, Accept-Ranges, X-SHA256) without consuming a download.",
)
async def head_drop(pin: str):
    """Allows clients and download managers to probe headers without decrementing downloads."""
    drop = await get_drop_by_pin(pin)
    if not drop or drop["status"] != "ACTIVE" or drop["expires_at"] <= datetime.now(timezone.utc):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Drop not found or expired.")

    return Response(
        status_code=status.HTTP_200_OK,
        headers={
            "Content-Disposition": f'attachment; filename="{drop["original_name"]}"',
            "Content-Type": drop["mime_type"],
            "Content-Length": str(drop["file_size_bytes"]),
            "Accept-Ranges": "bytes",
            "X-SHA256": drop["sha256_hash"],
            "X-Downloads-Remaining": str(drop["downloads_remaining"]),
        },
    )


@app.get(
    "/api/drops/{pin}",
    summary="Drop Metadata Query",
    description="Queries metadata for a drop without consuming a download.",
)
async def get_drop_metadata(pin: str):
    """Retrieves drop metadata without decrementing remaining downloads."""
    drop = await get_drop_by_pin(pin)
    if not drop:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Drop not found.")
    return {
        "pin": drop["pin"],
        "original_name": drop["original_name"],
        "file_size_bytes": drop["file_size_bytes"],
        "mime_type": drop["mime_type"],
        "sha256_hash": drop["sha256_hash"],
        "downloads_remaining": drop["downloads_remaining"],
        "max_downloads": drop["max_downloads"],
        "expires_at": drop["expires_at"].isoformat(),
        "status": drop["status"],
        "created_at": drop["created_at"].isoformat(),
    }


@app.get(
    "/api/drop/{pin}",
    summary="Drop Metadata Query (Alias)",
    description="Aliases /api/drops/{pin} to preview drop metadata without decrementing downloads.",
)
async def get_drop_metadata_alias(pin: str):
    """Singular alias for drop preview."""
    return await get_drop_metadata(pin)


@app.get(
    "/api/metrics",
    summary="Live System & Storage Metrics",
    description="Returns live statistics for active drops, total storage used, and janitor purge counts.",
)
async def get_system_live_metrics():
    """Returns aggregated real-time metrics for dashboard header."""
    metrics = await get_system_metrics()
    metrics["storage_used_formatted"] = format_size(metrics["storage_used_bytes"])
    metrics["public_base_url"] = settings.PUBLIC_BASE_URL
    return metrics


@app.get(
    "/d/{pin}",
    summary="Atomic Download Endpoint",
    description=(
        "Atomically claims a download using PostgreSQL atomic update. "
        "Supports HTTP Byte-Range requests (RFC 9110). "
        "Streams directly from MinIO with zero-RAM buffering. "
        "Solves the final-download deletion race condition by tracking in-flight readers and purging after the stream completes."
    ),
)
async def download_drop(pin: str, request: Request):
    """Atomic download endpoint with Byte-Range support and race-condition-free deletion."""
    # 1. Execute atomic UPDATE query to claim one download
    drop = await atomic_decrement_and_claim_drop(pin)

    # 2. Handle failure cases (exhausted, expired, purged, or not found)
    if not drop:
        existing = await get_drop_by_pin(pin)
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Drop not found.",
            )

        # Distinguish specific drop status for optimal client feedback
        if existing["status"] == "PURGED":
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail="This drop has already been purged from storage.",
            )
        elif existing["status"] == "DEPLETED" or existing["downloads_remaining"] <= 0:
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail="This drop has reached its maximum download limit.",
            )
        elif existing["expires_at"] <= datetime.now(timezone.utc):
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail="This drop has expired.",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_410_GONE,
                detail="This drop is no longer available.",
            )

    storage_key = drop["storage_key"]
    total_size = drop["file_size_bytes"]
    original_name = drop["original_name"]
    mime_type = drop["mime_type"]
    is_depleted = (drop["status"] == "DEPLETED" or drop["downloads_remaining"] == 0)

    # 3. HTTP Byte-Range Handling (RFC 9110)
    range_header = request.headers.get("range")
    if range_header:
        parsed_range = parse_range_header(range_header, total_size)
        if parsed_range is None:
            # Range header present but not satisfiable
            return Response(
                status_code=status.HTTP_416_RANGE_NOT_SATISFIABLE,
                headers={
                    "Content-Range": f"bytes */{total_size}",
                    "Accept-Ranges": "bytes",
                },
            )
        start, end = parsed_range
        content_length = end - start + 1
        s3_range = f"bytes={start}-{end}"
        status_code = status.HTTP_206_PARTIAL_CONTENT
        content_range_hdr = f"bytes {start}-{end}/{total_size}"
    else:
        content_length = total_size
        s3_range = None
        status_code = status.HTTP_200_OK
        content_range_hdr = None

    # 4. Construct Response Headers
    response_headers = {
        "Content-Disposition": f'attachment; filename="{original_name}"',
        "Content-Type": mime_type,
        "Content-Length": str(content_length),
        "Accept-Ranges": "bytes",
        "X-SHA256": drop["sha256_hash"],
        "X-Downloads-Remaining": str(drop["downloads_remaining"]),
    }
    if content_range_hdr:
        response_headers["Content-Range"] = content_range_hdr

    # 5. Final-Download Deletion Race Condition Solver
    # If this download depleted the drop, the object must be purged from MinIO
    # ONLY AFTER the streaming response finishes sending all bytes, and only if no other
    # concurrent readers are active.
    async def on_stream_finish(remaining_readers: int):
        if is_depleted and remaining_readers == 0:
            logger.info(
                "Final download completed for drop %s. Purging storage object '%s'...",
                pin,
                storage_key,
            )
            await delete_s3_object(storage_key)
            await mark_drop_purged(drop["id"])
            await send_discord_alert(drop, reason="DEPLETED")

    # 6. Stream from MinIO without buffering in RAM
    return StreamingResponse(
        content=get_s3_object_stream(
            storage_key=storage_key,
            byte_range=s3_range,
            on_finish=on_stream_finish,
        ),
        status_code=status_code,
        headers=response_headers,
    )
