"""MongoDB -> Snowflake ELT with dbt, for several source databases at once.

One DAG run fans out over every configured source (dynamic task mapping):

  get_sources ─► extract_changes[source] ─► load_to_snowflake[source] ─► commit_state[source] ─┐
                  (one mapped task instance per source: michu_coop, efoyta_wegagen, ...)       ├─► dbt_build
                                                                                               ┘
E  extract_changes   : incremental find() on updatedDate + change stream (with a stored resume token)
L  load_to_snowflake : write NDJSON change events -> PUT to an internal stage -> COPY INTO RAW.CUSTOMERS_RAW
   commit_state      : save that source's watermark / resume token ONLY after its load succeeded
T  dbt_build         : dbt models, snapshot and tests, once all sources are done

Sources come from MONGO_SOURCES ("mongo_db=label,mongo_db=label"). Each source keeps its
own watermark and resume token, so one source failing never moves another source's state.

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
from airflow.operators.python import get_current_context
from pymongo import MongoClient
from pymongo.errors import OperationFailure

from src.airflow_state import upsert_variable
from src.mongo_utils import normalize_mongo_document
from src.settings import get_settings
from src.snowflake_utils import copy_file_into_raw, get_snowflake_connection, put_file_to_stage
from src.sources import configured_sources

logger = logging.getLogger(__name__)


def _state_var(name: str, label: str) -> str:
    """Airflow Variable names are per source, e.g. sf_watermark__michu_coop."""
    return f"sf_{name}__{label}"


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


def _label_map_index(label: str) -> None:
    """Show the source label instead of 0, 1, 2 for each mapped task instance in the UI."""
    get_current_context()["source_label"] = label


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # never run two loads/merges on the same tables at once
    tags=["mongodb", "snowflake", "dbt", "customers", "elt"],
)
def mongo_snowflake_elt():
    @task
    def get_sources() -> list:
        sources = configured_sources()
        logger.info("Sources for this run: %s", sources)
        return sources

    @task(map_index_template="{{ source_label }}")
    def extract_changes(source: dict) -> dict:
        settings = get_settings()
        mongo_db, label = source["mongo_db"], source["label"]
        _label_map_index(label)

        client = MongoClient(settings.mongo_uri)
        collection = client[mongo_db][settings.mongo_collection]

        last_epoch = _parse_last_epoch(
            Variable.get(_state_var("watermark", label), default_var=""), settings.sync_lookback_minutes
        )
        initial_load = settings.initial_load and Variable.get(
            _state_var("initial_load_done", label), default_var="0"
        ) != "1"
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

        # Change stream on this source's collection, resumed from this source's stored token,
        # so deletes that happened between runs are not missed.
        raw_token = Variable.get(_state_var("resume_token", label), default_var="")
        watch_kwargs = {"full_document": "updateLookup"}
        if raw_token:
            watch_kwargs["resume_after"] = json.loads(raw_token)

        deadline = time.time() + settings.change_stream_max_seconds
        processed_events = 0
        try:
            stream = collection.watch(**watch_kwargs)
        except OperationFailure:
            # The token is older than the oplog window: we cannot resume. Start fresh and
            # flag it, because deletes in the gap are lost (run with FULL_REFRESH=1 to reconcile).
            logger.warning("[%s] resume token expired; starting a new change stream. Consider a full refresh.", label)
            stream = collection.watch(full_document="updateLookup")

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
        output_path = f"/opt/airflow/data/customers_{label}_{_get_safe_run_id()}.ndjson"
        with open(output_path, "w", encoding="utf-8") as handle:
            for doc_id, doc in upserts_by_id.items():
                handle.write(json.dumps({"op": "upsert", "source_database": label, "customer_id": doc_id,
                                         "doc": doc, "extracted_epoch": extracted_epoch}, ensure_ascii=True) + "\n")
            for doc_id in deletes:
                handle.write(json.dumps({"op": "delete", "source_database": label, "customer_id": doc_id,
                                         "doc": None, "extracted_epoch": extracted_epoch}, ensure_ascii=True) + "\n")

        logger.info("[%s] extracted upserts=%s deletes=%s change_events=%s -> %s",
                    label, len(upserts_by_id), len(deletes), processed_events, output_path)
        return {
            "label": label,
            "path": output_path,
            "events": len(upserts_by_id) + len(deletes),
            "max_updated_epoch": max_updated_epoch,
            "resume_token": json.dumps(new_resume_token) if new_resume_token else "",
            "initial_load": initial_load,
        }

    @task(map_index_template="{{ source_label }}")
    def load_to_snowflake(extract: dict) -> dict:
        _label_map_index(extract["label"])
        if extract["events"] == 0:
            logger.info("[%s] no changes to load", extract["label"])
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
            logger.info("[%s] loaded %s rows from %s into %s", extract["label"], loaded, staged_file, raw_table)
        finally:
            conn.close()
        return extract

    @task(map_index_template="{{ source_label }}")
    def commit_state(extract: dict) -> None:
        # Runs only after this source's raw load succeeded, so a failed load is simply
        # retried next run, and other sources are unaffected.
        label = extract["label"]
        _label_map_index(label)
        upsert_variable(_state_var("watermark", label), str(extract["max_updated_epoch"]))
        if extract["resume_token"]:
            upsert_variable(_state_var("resume_token", label), extract["resume_token"])
        if extract["initial_load"]:
            upsert_variable(_state_var("initial_load_done", label), "1")

    # Read at parse time from the environment (not get_settings()), so parsing the DAG
    # never needs Mongo/Snowflake credentials.
    dbt_project_dir = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")
    dbt_bin = os.environ.get("DBT_BIN", "/opt/airflow/dbt_venv/bin/dbt")
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {dbt_project_dir} && {dbt_bin} build --profiles-dir {dbt_project_dir} --target prod",
        append_env=True,
        # Transform whatever was loaded even if one source failed this hour: the dbt models are
        # incremental and the failed source is simply picked up on its next successful run.
        trigger_rule="all_done",
    )

    extracted = extract_changes.expand(source=get_sources())
    loaded = load_to_snowflake.expand(extract=extracted)
    commit_state.expand(extract=loaded) >> dbt_build


mongo_snowflake_elt()
