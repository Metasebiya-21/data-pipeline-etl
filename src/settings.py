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
    # Label stored with every record in Snowflake (defaults to the Mongo database name)
    source_database: str = Field("", alias="SOURCE_DATABASE")

    # Oracle target (mongo_oracle_etl DAG)
    oracle_user: str = Field("", alias="ORACLE_USER")
    oracle_password: str = Field("", alias="ORACLE_PASSWORD")
    oracle_dsn: str = Field("", alias="ORACLE_DSN")
    oracle_table: str = Field("", alias="ORACLE_TABLE")
    oracle_connect_mode: str = Field("auto", alias="ORACLE_CONNECT_MODE")
    oracle_client_lib_dir: str = Field("", alias="ORACLE_CLIENT_LIB_DIR")

    # Snowflake target (mongo_snowflake_elt DAG)
    snowflake_account: str = Field("", alias="SNOWFLAKE_ACCOUNT")
    snowflake_user: str = Field("", alias="SNOWFLAKE_USER")
    snowflake_password: str = Field("", alias="SNOWFLAKE_PASSWORD")
    snowflake_role: str = Field("LOADER", alias="SNOWFLAKE_LOADER_ROLE")
    snowflake_warehouse: str = Field("LOAD_WH", alias="SNOWFLAKE_LOAD_WAREHOUSE")
    snowflake_database: str = Field("LENDING", alias="SNOWFLAKE_DATABASE")
    snowflake_raw_schema: str = Field("RAW", alias="SNOWFLAKE_RAW_SCHEMA")
    snowflake_stage: str = Field("CUSTOMERS_STAGE", alias="SNOWFLAKE_STAGE")
    snowflake_raw_table: str = Field("CUSTOMERS_RAW", alias="SNOWFLAKE_RAW_TABLE")
    dbt_project_dir: str = Field("/opt/airflow/dbt", alias="DBT_PROJECT_DIR")
    dbt_bin: str = Field("/opt/airflow/dbt_venv/bin/dbt", alias="DBT_BIN")

    sync_lookback_minutes: int = Field(1440, alias="SYNC_LOOKBACK_MINUTES")
    change_stream_max_seconds: int = Field(60, alias="CHANGE_STREAM_MAX_SECONDS")
    change_stream_max_events: int = Field(500, alias="CHANGE_STREAM_MAX_EVENTS")
    full_refresh: bool = Field(False, alias="FULL_REFRESH")
    initial_load: bool = Field(False, alias="INITIAL_LOAD")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
