import logging
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # Application Configuration
    APP_PORT: int = 8080
    PORT: Optional[int] = None
    PUBLIC_BASE_URL: str = "http://localhost:8080"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    DISCORD_WEBHOOK_URL: Optional[str] = None
    JANITOR_INTERVAL_SECONDS: int = 30

    # PostgreSQL Database Configuration
    DATABASE_URL: Optional[str] = None
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "qrdrop"
    POSTGRES_HOST: str = "postgres"
    POSTGRES_PORT: int = 5432
    DATABASE_POOL_MIN_SIZE: int = 2
    DATABASE_POOL_MAX_SIZE: int = 10

    # MinIO / S3 Object Storage Configuration
    MINIO_ENDPOINT: str = "http://minio:9000"
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_BUCKET: str = "qrdrop-blobs"
    MINIO_REGION: str = "us-east-1"
    MINIO_USE_SSL: bool = False

    @property
    def database_dsn(self) -> str:
        if self.DATABASE_URL:
            url = self.DATABASE_URL.strip()
            if url.startswith("postgres://"):
                url = "postgresql://" + url[len("postgres://"):]
            return url
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def effective_port(self) -> int:
        if self.PORT is not None:
            return self.PORT
        return self.APP_PORT

    @property
    def is_s3_ssl(self) -> bool:
        return self.MINIO_USE_SSL or self.MINIO_ENDPOINT.startswith("https://")


settings = Settings()

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
