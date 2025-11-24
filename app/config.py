from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_BUCKET: str = "uploads"

    MONGO_URI: str = "mongodb://mongo:27017"
    MONGO_DB: str = "file_db"
    MONGO_META_COLLECTION: str = "file_meta"
    MONGO_JOBS_COLLECTION: str = "upload_jobs"

    APP_MODULE: str = "app.app:app"
    PORT: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
    )


settings = Settings()
