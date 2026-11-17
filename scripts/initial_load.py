"""One-time initial load: MongoDB databases -> Snowflake RAW.CUSTOMERS_RAW.

For each source database it streams the customers collection into NDJSON files
(one change event per line), PUTs each file to the internal stage and loads it
with COPY INTO. Re-running is safe: COPY INTO skips files it already loaded.

Usage (from the repo root, with .env filled in):
    set -a; source .env; set +a
    python -m scripts.initial_load
"""
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone

from pymongo import MongoClient

from src.mongo_utils import normalize_mongo_document
from src.snowflake_utils import auth_kwargs
from src.sources import configured_sources

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("initial_load")

DOCS_PER_FILE = int(os.environ.get("DOCS_PER_FILE", "25000"))
DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "LENDING")
STAGE = f"{DATABASE}.RAW.CUSTOMERS_STAGE"
RAW_TABLE = f"{DATABASE}.RAW.CUSTOMERS_RAW"


def snowflake_connection():
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        **auth_kwargs(
            os.environ.get("SNOWFLAKE_PASSWORD", ""),
            os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE", ""),
            os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE_PWD", ""),
        ),
        role=os.environ.get("SNOWFLAKE_LOADER_ROLE", "LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_LOAD_WAREHOUSE", "LOAD_WH"),
        database=DATABASE,
        schema="RAW",
        session_parameters={"QUERY_TAG": "initial_load"},
    )


def check_session(cursor) -> None:
    """Fail fast if the role cannot use the warehouse or database.

    Snowflake does not error at login when the role lacks USAGE on the requested
    warehouse/database: it silently leaves them unset, and the first PUT/COPY fails later.
    """
    user, role, warehouse, database, schema = cursor.execute(
        "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()"
    ).fetchone()
    logger.info("Session: user=%s role=%s warehouse=%s database=%s schema=%s", user, role, warehouse, database, schema)
    missing = [name for name, value in (("warehouse", warehouse), ("database", database), ("schema", schema)) if not value]
    if missing:
        raise SystemExit(
            f"Role {role} cannot use the {', '.join(missing)}. Grant USAGE to the role "
            f"(see section 6 of snowflake/01_setup.sql) and run again."
        )


def write_batches(collection, label: str, out_dir: str):
    """Yield paths of NDJSON files with up to DOCS_PER_FILE documents each."""
    extracted_epoch = int(datetime.now(timezone.utc).timestamp())
    batch_no, count, handle, path = 0, 0, None, None
    for doc in collection.find({}, batch_size=2000):
        if handle is None:
            batch_no += 1
            path = os.path.join(out_dir, f"{label}_{extracted_epoch}_{batch_no:04d}.ndjson")
            handle = open(path, "w", encoding="utf-8")
        normalized = normalize_mongo_document(doc)
        handle.write(json.dumps({
            "op": "upsert",
            "source_database": label,
            "customer_id": str(normalized.get("_id")),
            "doc": normalized,
            "extracted_epoch": extracted_epoch,
        }, ensure_ascii=True) + "\n")
        count += 1
        if count % DOCS_PER_FILE == 0:
            handle.close()
            yield path
            handle = None
    if handle is not None:
        handle.close()
        yield path


def load_file(cursor, path: str) -> int:
    cursor.execute(f"PUT 'file://{path}' @{STAGE} AUTO_COMPRESS=TRUE OVERWRITE=FALSE")
    staged_file = cursor.fetchall()[0][1]
    cursor.execute(f"""
        COPY INTO {RAW_TABLE} (change_type, source_database, customer_id, doc, extracted_epoch,
                                 _source_file, _source_row, _loaded_at)
        FROM (
            SELECT $1:op::string, $1:source_database::string, $1:customer_id::string, $1:doc,
                   $1:extracted_epoch::number, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
            FROM @{STAGE}
        )
        FILES = ('{staged_file}')
        ON_ERROR = 'ABORT_STATEMENT'
    """)
    columns = [c[0].lower() for c in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return sum(r.get("rows_loaded") or 0 for r in results)


def main():
    mongo = MongoClient(os.environ["MONGO_URI"])
    collection_name = os.environ.get("MONGO_COLLECTION", "customers")
    sources = configured_sources()

    conn = snowflake_connection()
    try:
        cursor = conn.cursor()
        check_session(cursor)
        with tempfile.TemporaryDirectory() as out_dir:
            for source in sources:
                mongo_db, label = source["mongo_db"], source["label"]
                collection = mongo[mongo_db][collection_name]
                expected = collection.estimated_document_count()
                logger.info("Loading %s.%s as '%s' (~%s documents)", mongo_db, collection_name, label, expected)
                started, loaded = time.time(), 0
                for path in write_batches(collection, label, out_dir):
                    loaded += load_file(cursor, path)
                    os.remove(path)
                    logger.info("  %s: %s rows loaded so far", label, loaded)
                logger.info("Done %s: %s rows in %.0fs (Mongo reported ~%s)", label, loaded, time.time() - started, expected)

        # Reconciliation: rows per source in Snowflake vs. what Mongo reported
        cursor.execute(f"SELECT source_database, COUNT(*) FROM {RAW_TABLE} GROUP BY 1 ORDER BY 1")
        for source, rows in cursor.fetchall():
            logger.info("Snowflake RAW.CUSTOMERS_RAW: %s = %s rows", source, rows)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
