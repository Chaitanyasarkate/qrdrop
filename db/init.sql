-- QR-Drop: Database Schema & Initialization
-- Target PostgreSQL: 16+

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS drops (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pin VARCHAR(6) UNIQUE NOT NULL,
    original_name TEXT NOT NULL,
    storage_key TEXT UNIQUE NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    mime_type VARCHAR(128) NOT NULL DEFAULT 'application/octet-stream',
    sha256_hash CHAR(64) NOT NULL,
    downloads_remaining INT NOT NULL DEFAULT 3,
    max_downloads INT NOT NULL DEFAULT 3,
    expires_at TIMESTAMPTZ NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_drop_status CHECK (status IN ('ACTIVE', 'DEPLETED', 'EXPIRED', 'PURGED'))
);

-- Partial index for high-throughput lookup of active pins (upload confirmation & download requests)
CREATE INDEX IF NOT EXISTS idx_drops_pin_active ON drops (pin) WHERE status = 'ACTIVE';

-- Partial index for background reaper service to purge expired or depleted drops efficiently
CREATE INDEX IF NOT EXISTS idx_drops_expires_at_active_depleted ON drops (expires_at) WHERE status IN ('ACTIVE', 'DEPLETED');
