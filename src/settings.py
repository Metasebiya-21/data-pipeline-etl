from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="/opt/airflow/.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    mongo_uri: str = Field(..., alias="MONGO_URI")
    mongo_db: str = Field(..., alias="MONGO_DB")
    mongo_collection: str = Field(..., alias="MONGO_COLLECTION")
    mongo_change_stream_db: str = Field(..., alias="MONGO_CHANGE_STREAM_DB")
    mongo_change_stream_collection: str = Field(..., alias="MONGO_CHANGE_STREAM_COLLECTION")

    oracle_user: str = Field(..., alias="ORACLE_USER")
    oracle_password: str = Field(..., alias="ORACLE_PASSWORD")
    oracle_dsn: str = Field(..., alias="ORACLE_DSN")
    oracle_table: str = Field(..., alias="ORACLE_TABLE")
    oracle_connect_mode: str = Field("auto", alias="ORACLE_CONNECT_MODE")
    oracle_client_lib_dir: str = Field("", alias="ORACLE_CLIENT_LIB_DIR")

    sync_lookback_minutes: int = Field(1440, alias="SYNC_LOOKBACK_MINUTES")
    change_stream_max_seconds: int = Field(60, alias="CHANGE_STREAM_MAX_SECONDS")
    change_stream_max_events: int = Field(500, alias="CHANGE_STREAM_MAX_EVENTS")
    full_refresh: bool = Field(False, alias="FULL_REFRESH")
    initial_load: bool = Field(False, alias="INITIAL_LOAD")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
