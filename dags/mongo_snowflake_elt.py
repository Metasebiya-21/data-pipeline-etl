"""MongoDB -> Snowflake ELT with dbt.

E  extract_changes   : incremental find() on updatedDate + change stream (with a stored resume token)
L  load_to_snowflake : write NDJSON change events -> PUT to an internal stage -> COPY INTO RAW.CUSTOMERS_RAW
   commit_state      : save the watermark / resume token ONLY after the load succeeded
T  dbt_build         : dbt models (staging -> intermediate -> marts), tests and snapshots inside Snowflake

See docs/snowflake_dbt_elt.md for the walkthrough.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from src.airflow_state import upsert_variable
from src.mongo_utils import normalize_mongo_document
from src.settings import get_settings
from src.snowflake_utils import copy_file_into_raw, get_snowflake_connection, put_file_to_stage

logger = logging.getLogger(__name__)

WATERMARK_VAR = "sf_customers_last_updated_epoch"
RESUME_TOKEN_VAR = "sf_customers_change_stream_resume_token"
INITIAL_LOAD_DONE_VAR = "sf_customers_initial_load_done"


def _epoch_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _parse_last_epoch(last_epoch_str: str, lookback_minutes: int) -> int:
    if last_epoch_str:
        try:
            return int(last_epoch_str)
        except ValueError:
            pass
    return _epoch_now() - (lookback_minutes * 60)


def _get_safe_run_id() -> str:
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    return run_id.replace(":", "_").replace("/", "_").replace("+", "_")


def _initial_load_enabled(settings) -> bool:
    if not settings.initial_load:
        return False
    return Variable.get(INITIAL_LOAD_DONE_VAR, default_var="0") != "1"


def _load_resume_token():
    raw = Variable.get(RESUME_TOKEN_VAR, default_var="")
    return json.loads(raw) if raw else None


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # never run two loads/merges on the same tables at once
    tags=["mongodb", "snowflake", "dbt", "customers", "elt"],
)
def mongo_snowflake_elt():
    @task
    def extract_changes() -> dict:
        settings = get_settings()
        client = MongoClient(settings.mongo_uri)
        collection = client[settings.mongo_db][settings.mongo_collection]

        last_epoch = _parse_last_epoch(Variable.get(WATERMARK_VAR, default_var=""), settings.sync_lookback_minutes)
        initial_load = _initial_load_enabled(settings)
        extracted_epoch = _epoch_now()

        upserts_by_id = {}
        deletes = set()
        max_updated_epoch = last_epoch

        if settings.full_refresh or initial_load:
            cursor = collection.find({})
        else:
            cursor = collection.find(
                {
                    "$or": [
                        {"updatedDate": {"$gte": last_epoch}},
                        {"updatedDate": {"$exists": False}, "createdDate": {"$gte": last_epoch}},
                    ]
                }
            )

        for doc in cursor:
            normalized = normalize_mongo_document(doc)
            doc_id = str(normalized.get("_id"))
            upserts_by_id[doc_id] = normalized
            updated_epoch = normalized.get("updatedDate")
            if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
                max_updated_epoch = updated_epoch

        # Change stream: resume from the stored token so deletes that happened
        # between runs are not missed.
        change_collection = client[settings.mongo_change_stream_db][settings.mongo_change_stream_collection]
        resume_token = _load_resume_token()
        watch_kwargs = {"full_document": "updateLookup"}
        if resume_token:
            watch_kwargs["resume_after"] = resume_token

        deadline = time.time() + settings.change_stream_max_seconds
        processed_events = 0
        try:
            stream = change_collection.watch(**watch_kwargs)
        except OperationFailure:
            # The token is older than the oplog window: we cannot resume. Start fresh and
            # flag it, because deletes in the gap are lost (run with FULL_REFRESH=1 to reconcile).
            logger.warning("Change stream resume token expired; starting a new stream. Consider a full refresh.")
            stream = change_collection.watch(full_document="updateLookup")

        with stream:
            while time.time() < deadline and processed_events < settings.change_stream_max_events:
                event = stream.try_next()
                if event is None:
                    time.sleep(1)
                    continue
                processed_events += 1
                op_type = event.get("operationType")
                if op_type in {"insert", "update", "replace"} and event.get("fullDocument"):
                    normalized = normalize_mongo_document(event["fullDocument"])
                    doc_id = str(normalized.get("_id"))
                    upserts_by_id[doc_id] = normalized
                    deletes.discard(doc_id)
                    updated_epoch = normalized.get("updatedDate")
                    if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
                        max_updated_epoch = updated_epoch
                elif op_type == "delete" and "_id" in event.get("documentKey", {}):
                    doc_id = str(event["documentKey"]["_id"])
                    deletes.add(doc_id)
                    upserts_by_id.pop(doc_id, None)
            new_resume_token = stream.resume_token

        # One JSON object per line (NDJSON): one change event per customer.
        output_path = f"/opt/airflow/data/customers_{_get_safe_run_id()}.ndjson"
        with open(output_path, "w", encoding="utf-8") as handle:
            for doc_id, doc in upserts_by_id.items():
                handle.write(json.dumps({"op": "upsert", "customer_id": doc_id, "doc": doc,
                                         "extracted_epoch": extracted_epoch}, ensure_ascii=True) + "\n")
            for doc_id in deletes:
                handle.write(json.dumps({"op": "delete", "customer_id": doc_id, "doc": None,
                                         "extracted_epoch": extracted_epoch}, ensure_ascii=True) + "\n")

        logger.info("Extracted upserts=%s deletes=%s change_events=%s -> %s",
                    len(upserts_by_id), len(deletes), processed_events, output_path)
        return {
            "path": output_path,
            "events": len(upserts_by_id) + len(deletes),
            "max_updated_epoch": max_updated_epoch,
            "resume_token": json.dumps(new_resume_token) if new_resume_token else "",
            "initial_load": initial_load,
        }

    @task
    def load_to_snowflake(extract: dict) -> dict:
        if extract["events"] == 0:
            logger.info("No changes to load")
            return extract

        settings = get_settings()
        stage = f"{settings.snowflake_raw_schema}.{settings.snowflake_stage}"
        raw_table = f"{settings.snowflake_raw_schema}.{settings.snowflake_raw_table}"

        conn = get_snowflake_connection(settings)
        try:
            cursor = conn.cursor()
            staged_file = put_file_to_stage(cursor, extract["path"], stage)
            results = copy_file_into_raw(cursor, raw_table, stage, staged_file)
            loaded = sum(r.get("rows_loaded") or 0 for r in results)
            logger.info("Loaded %s rows from %s into %s", loaded, staged_file, raw_table)
        finally:
            conn.close()
        return extract

    @task
    def commit_state(extract: dict) -> None:
        # Runs only after the raw load succeeded, so a failed load is simply retried next run.
        upsert_variable(WATERMARK_VAR, str(extract["max_updated_epoch"]))
        if extract["resume_token"]:
            upsert_variable(RESUME_TOKEN_VAR, extract["resume_token"])
        if extract["initial_load"]:
            upsert_variable(INITIAL_LOAD_DONE_VAR, "1")

    # Read at parse time from the environment (not get_settings()), so parsing the DAG
    # never needs Mongo/Snowflake credentials.
    dbt_project_dir = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    dbt_bin = os.environ.get("DBT_BIN", "/opt/airflow/dbt_venv/bin/dbt")
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {dbt_project_dir} && {dbt_bin} build --profiles-dir {dbt_project_dir} --target prod",
        append_env=True,
    )

    extracted = extract_changes()
    loaded = load_to_snowflake(extracted)
    commit_state(loaded) >> dbt_build


mongo_snowflake_elt()
