import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
import httpx

from config import settings
from database import (
    expire_past_due_drops,
    get_drops_ready_for_purge,
    mark_drop_purged,
)
from storage import delete_s3_object, is_storage_key_in_use
from qr import format_size

logger = logging.getLogger("qrdrop.janitor")


async def send_discord_alert(drop: dict, reason: str) -> None:
    """Dispatches a Discord webhook notification when a drop is purged."""
    webhook_url = settings.DISCORD_WEBHOOK_URL
    if not webhook_url or not webhook_url.startswith("http"):
        return

    # Green for Depleted, Amber/Orange for Expired
    color = 0x10B981 if reason == "DEPLETED" else 0xF59E0B

    now_iso = datetime.now(timezone.utc).isoformat()
    embed = {
        "title": "🗑️ Storage Janitor: Drop Purged",
        "description": f"Ephemeral drop **{drop.get('original_name')}** has been purged from MinIO object storage.",
        "color": color,
        "fields": [
            {"name": "PIN Code", "value": f"`{drop.get('pin')}`", "inline": True},
            {"name": "Reason", "value": reason, "inline": True},
            {"name": "File Size", "value": format_size(drop.get("file_size_bytes", 0)), "inline": True},
            {"name": "SHA-256", "value": f"`{drop.get('sha256_hash')}`", "inline": False},
        ],
        "footer": {"text": "QR-Drop Ephemeral Engine • Storage Janitor"},
        "timestamp": now_iso,
    }

    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.post(webhook_url, json={"embeds": [embed]})
            if resp.status_code >= 400:
                logger.warning("Discord webhook returned status %s: %s", resp.status_code, resp.text)
            else:
                logger.info("Dispatched Discord alert for drop %s (%s)", drop.get("pin"), reason)
    except Exception as e:
        logger.warning("Failed to dispatch Discord webhook alert: %s", e)


async def purge_single_drop(drop: dict, reason: str) -> bool:
    """Safely purges an S3 object, updates the DB to 'PURGED', and sends an alert."""
    storage_key = drop["storage_key"]
    if is_storage_key_in_use(storage_key):
        logger.info(
            "Skipping purge for drop %s: object '%s' is actively streaming.",
            drop.get("pin"),
            storage_key,
        )
        return False

    logger.info("Purging drop %s ('%s', reason: %s)...", drop.get("pin"), storage_key, reason)

    # Delete from MinIO
    await delete_s3_object(storage_key)

    # Mark as PURGED in PostgreSQL
    await mark_drop_purged(drop["id"])

    # Dispatch Discord alert
    await send_discord_alert(drop, reason=reason)
    return True


async def run_janitor_pass() -> int:
    """Executes a single janitor maintenance cycle."""
    purged_count = 0

    try:
        # Step 1: Expire drops whose expires_at is past
        expired_drops = await expire_past_due_drops()
        if expired_drops:
            logger.info("Storage Janitor transitioned %d active drops to EXPIRED.", len(expired_drops))

        # Step 2: Retrieve all drops in 'EXPIRED' or 'DEPLETED' status
        candidates = await get_drops_ready_for_purge()
        for drop in candidates:
            success = await purge_single_drop(drop, reason=drop["status"])
            if success:
                purged_count += 1

        if purged_count > 0:
            logger.info("Storage Janitor cycle completed: %d drop(s) purged from MinIO.", purged_count)

    except Exception as e:
        logger.error("Error during Storage Janitor cycle: %s", e)

    return purged_count


async def storage_janitor_worker() -> None:
    """Long-running background task executing janitor cycles periodically."""
    interval = max(5, settings.JANITOR_INTERVAL_SECONDS)
    logger.info("Starting Storage Janitor worker (interval: %ds)...", interval)

    try:
        while True:
            await asyncio.sleep(interval)
            await run_janitor_pass()
    except asyncio.CancelledError:
        logger.info("Storage Janitor worker received cancellation signal. Shutting down worker...")
    except Exception as e:
        logger.error("Storage Janitor worker encountered fatal error: %s", e)
