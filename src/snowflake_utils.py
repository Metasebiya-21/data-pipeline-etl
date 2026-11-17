import logging
import os
from typing import Any, Dict, List

from src.settings import Settings

logger = logging.getLogger(__name__)


def get_snowflake_connection(settings: Settings) -> Any:
    import snowflake.connector

    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        role=settings.snowflake_role,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        schema=settings.snowflake_raw_schema,
        session_parameters={"QUERY_TAG": "airflow_mongo_snowflake_elt"},
    )


def put_file_to_stage(cursor: Any, local_path: str, stage: str) -> str:
    """Upload a local file to an internal stage. Returns the staged file name."""
    cursor.execute(f"PUT 'file://{local_path}' @{stage} AUTO_COMPRESS=TRUE OVERWRITE=FALSE")
    rows = cursor.fetchall()
    logger.info("PUT result: %s", rows)
    # PUT returns the target file name in the second column (e.g. file.ndjson.gz)
    return rows[0][1] if rows else os.path.basename(local_path) + ".gz"


def copy_file_into_raw(cursor: Any, raw_table: str, stage: str, staged_file: str) -> List[Dict[str, Any]]:
    """Load one staged NDJSON file into the raw table.

    COPY INTO keeps 64 days of load metadata per table, so running this again for a
    file that was already loaded is a no-op: Airflow retries cannot create duplicates.
    """
    cursor.execute(
        f"""
        COPY INTO {raw_table} (change_type, source_database, customer_id, doc, extracted_epoch,
                               _source_file, _source_row, _loaded_at)
        FROM (
            SELECT $1:op::string,
                   $1:source_database::string,
                   $1:customer_id::string,
                   $1:doc,
                   $1:extracted_epoch::number,
                   METADATA$FILENAME,
                   METADATA$FILE_ROW_NUMBER,
                   CURRENT_TIMESTAMP()
            FROM @{stage}
        )
        FILES = ('{staged_file}')
        ON_ERROR = 'ABORT_STATEMENT'
        """
    )
    columns = [c[0].lower() for c in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    logger.info("COPY INTO result: %s", results)
    return results
