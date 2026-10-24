import logging
from typing import AsyncGenerator, Optional
import asyncpg
from config import settings

logger = logging.getLogger("qrdrop.database")

pool: Optional[asyncpg.Pool] = None


async def init_db_schema() -> None:
    """Auto-creates tables, pgcrypto extension, and partial indexes if missing."""
    global pool
    if pool is None:
        return
    schema_sql = """
    CREATE EXTENSION IF NOT EXISTS "pgcrypto";

    CREATE TABLE IF NOT EXISTS drops (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        pin VARCHAR(6) UNIQUE NOT NULL,
        original_name TEXT NOT NULL,
        storage_key TEXT UNIQUE NOT NULL,
        file_size_bytes BIGINT NOT NULL,
        mime_type VARCHAR(128) NOT NULL DEFAULT 'application/octet-stream',
        sha256_hash CHAR(64) NOT NULL,
        downloads_remaining INTEGER NOT NULL DEFAULT 1,
        max_downloads INTEGER NOT NULL DEFAULT 1,
        status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        purged_at TIMESTAMPTZ,
        CONSTRAINT chk_status CHECK (status IN ('ACTIVE', 'DEPLETED', 'PURGED')),
        CONSTRAINT chk_downloads_remaining CHECK (downloads_remaining >= 0),
        CONSTRAINT chk_max_downloads CHECK (max_downloads > 0)
    );

    CREATE INDEX IF NOT EXISTS idx_drops_pin_active ON drops(pin) WHERE status = 'ACTIVE';
    CREATE INDEX IF NOT EXISTS idx_drops_expires_at_active_depleted ON drops(expires_at) WHERE status IN ('ACTIVE', 'DEPLETED');
    """
    async with pool.acquire() as conn:
        await conn.execute(schema_sql)
    logger.info("Database schema verified and initialized.")


async def init_db_pool() -> asyncpg.Pool:
    """Initializes the asyncpg connection pool with PostgreSQL."""
    global pool
    logger.info("Initializing asyncpg connection pool...")
    pool = await asyncpg.create_pool(
        dsn=settings.database_dsn,
        min_size=settings.DATABASE_POOL_MIN_SIZE,
        max_size=settings.DATABASE_POOL_MAX_SIZE,
    )
    logger.info("asyncpg connection pool successfully established.")
    await init_db_schema()
    return pool


async def close_db_pool() -> None:
    """Closes all active connections in the asyncpg pool."""
    global pool
    if pool is not None:
        logger.info("Closing asyncpg connection pool...")
        await pool.close()
        pool = None
        logger.info("asyncpg connection pool closed.")


def get_db_pool() -> asyncpg.Pool:
    """Returns the initialized asyncpg connection pool."""
    if pool is None:
        raise RuntimeError("Database pool has not been initialized. Lifespan context missing?")
    return pool


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """FastAPI dependency yielding an asyncpg connection from the pool."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as connection:
        yield connection


async def check_db_health() -> bool:
    """Executes a lightweight query to confirm database responsiveness."""
    try:
        db_pool = get_db_pool()
        result = await db_pool.fetchval("SELECT 1;")
        return result == 1
    except Exception as e:
        logger.error("Database healthcheck query failed: %s", e)
        return False


async def generate_unique_pin(max_attempts: int = 30) -> str:
    """
    Generates a cryptographically secure 6-digit numeric PIN (000000-999999).
    Guarantees no collision among ACTIVE drops (and unique table-wide).
    """
    import secrets

    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        for _ in range(max_attempts):
            candidate = f"{secrets.randbelow(1_000_000):06d}"
            # Check if candidate PIN is already taken by any active drop or existing drop
            exists = await conn.fetchval(
                "SELECT 1 FROM drops WHERE pin = $1 AND status = 'ACTIVE' LIMIT 1;",
                candidate,
            )
            if not exists:
                # Also verify table constraint uniqueness
                table_exists = await conn.fetchval(
                    "SELECT 1 FROM drops WHERE pin = $1 LIMIT 1;",
                    candidate,
                )
                if not table_exists:
                    return candidate

    raise RuntimeError("Failed to allocate a unique 6-digit PIN after maximum attempts.")


async def create_drop_record(
    pin: str,
    original_name: str,
    storage_key: str,
    file_size_bytes: int,
    mime_type: str,
    sha256_hash: str,
    max_downloads: int,
    expires_at,
) -> dict:
    """Inserts a new ACTIVE drop record into PostgreSQL and returns the row as a dict."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO drops (
                pin,
                original_name,
                storage_key,
                file_size_bytes,
                mime_type,
                sha256_hash,
                downloads_remaining,
                max_downloads,
                expires_at,
                status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, 'ACTIVE')
            RETURNING *;
            """,
            pin,
            original_name,
            storage_key,
            file_size_bytes,
            mime_type,
            sha256_hash,
            max_downloads,
            expires_at,
        )
        return dict(row)


async def get_drop_by_pin(pin: str) -> Optional[dict]:
    """Retrieves a drop by its 6-digit PIN."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM drops WHERE pin = $1 LIMIT 1;",
            pin,
        )
        return dict(row) if row else None


async def atomic_decrement_and_claim_drop(pin: str) -> Optional[dict]:
    """
    Executes a single atomic UPDATE query to claim a download, decrementing
    downloads_remaining and setting status to 'DEPLETED' if it reaches 0.
    Prevents race conditions if multiple clients download concurrently.
    """
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE drops
            SET downloads_remaining = downloads_remaining - 1,
                status = CASE WHEN downloads_remaining - 1 = 0 THEN 'DEPLETED' ELSE 'ACTIVE' END
            WHERE pin = $1
              AND status = 'ACTIVE'
              AND downloads_remaining > 0
              AND expires_at > NOW()
            RETURNING *;
            """,
            pin,
        )
        return dict(row) if row else None


async def mark_drop_purged(drop_id) -> None:
    """Updates drop status to 'PURGED' after object deletion from MinIO."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE drops SET status = 'PURGED' WHERE id = $1;",
            drop_id,
        )


async def expire_past_due_drops() -> list[dict]:
    """Transitions active drops past their expires_at into 'EXPIRED' status."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE drops
            SET status = 'EXPIRED'
            WHERE status = 'ACTIVE'
              AND expires_at <= NOW()
            RETURNING *;
            """
        )
        return [dict(r) for r in rows]


async def get_drops_ready_for_purge() -> list[dict]:
    """Retrieves all drops with status in ('EXPIRED', 'DEPLETED') ready to be purged."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM drops
            WHERE status IN ('EXPIRED', 'DEPLETED')
            ORDER BY created_at ASC;
            """
        )
        return [dict(r) for r in rows]


async def get_system_metrics() -> dict:
    """Queries live metrics for active drops, total storage used in S3, and janitor purge counts."""
    db_pool = get_db_pool()
    async with db_pool.acquire() as conn:
        active_count = await conn.fetchval(
            "SELECT COUNT(*) FROM drops WHERE status = 'ACTIVE' AND expires_at > NOW();"
        )
        storage_bytes = await conn.fetchval(
            "SELECT COALESCE(SUM(file_size_bytes), 0) FROM drops WHERE status IN ('ACTIVE', 'DEPLETED');"
        )
        purged_count = await conn.fetchval(
            "SELECT COUNT(*) FROM drops WHERE status = 'PURGED';"
        )
        total_drops = await conn.fetchval(
            "SELECT COUNT(*) FROM drops;"
        )
        total_downloads = await conn.fetchval(
            "SELECT COALESCE(SUM(max_downloads - downloads_remaining), 0) FROM drops;"
        )
        return {
            "active_files": int(active_count or 0),
            "storage_used_bytes": int(storage_bytes or 0),
            "total_purged": int(purged_count or 0),
            "total_drops": int(total_drops or 0),
            "total_downloads": int(total_downloads or 0),
        }



