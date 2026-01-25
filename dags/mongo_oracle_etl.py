import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.session import create_session
from pymongo import MongoClient
from sqlalchemy.exc import IntegrityError

from src.customer_etl import build_customer_payload, load_customer
from src.mongo_utils import normalize_mongo_document
from src.oracle_utils import get_oracle_connection
from src.settings import get_settings

logger = logging.getLogger(__name__)

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except Exception:
    pg_insert = None


def _epoch_now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _parse_last_epoch(last_epoch_str: str, lookback_minutes: int) -> int:
    if last_epoch_str:
        try:
            return int(last_epoch_str)
        except ValueError:
            return _epoch_now() - (lookback_minutes * 60)
    return _epoch_now() - (lookback_minutes * 60)


def _get_safe_run_id() -> str:
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    return run_id.replace(":", "_").replace("/", "_")


def _write_changes(payload: dict) -> str:
    output_path = f"/opt/airflow/data/mongo_oracle_{_get_safe_run_id()}.json"
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)
    return output_path


def _initial_load_enabled(settings) -> bool:
    if not settings.initial_load:
        return False
    done_flag = Variable.get("customers_initial_load_done", default_var="0")
    return done_flag != "1"


def _upsert_variable(key: str, value: str) -> None:
    with create_session() as session:
        dialect = session.get_bind().dialect.name
        if pg_insert and dialect == "postgresql":
            stmt = pg_insert(Variable).values(
                key=key,
                val=value,
                description=None,
                is_encrypted=False,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["key"],
                set_={
                    "val": value,
                    "description": None,
                    "is_encrypted": False,
                },
            )
            session.execute(stmt)
            session.flush()
            return

        try:
            Variable.set(key, value, session=session)
        except IntegrityError:
            session.rollback()
            session.query(Variable).filter(Variable.key == key).update(
                {
                    "val": value,
                    "description": None,
                    "is_encrypted": False,
                }
            )
            session.flush()


@dag(
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mongodb", "oracle", "customers", "etl"],
)
def mongo_oracle_etl():
    @task
    def extract_changes() -> str:
        settings = get_settings()
        client = MongoClient(settings.mongo_uri)
        db = client[settings.mongo_db]
        collection = db[settings.mongo_collection]

        last_epoch_str = Variable.get("customers_last_updated_epoch", default_var="")
        last_epoch = _parse_last_epoch(last_epoch_str, settings.sync_lookback_minutes)

        upserts_by_id = {}
        deletes = set()
        max_updated_epoch = last_epoch

        initial_load = _initial_load_enabled(settings)
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
            deletes.discard(doc_id)
            updated_epoch = normalized.get("updatedDate")
            if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
                max_updated_epoch = updated_epoch

        change_db = client[settings.mongo_change_stream_db]
        change_collection = change_db[settings.mongo_change_stream_collection]
        deadline = time.time() + settings.change_stream_max_seconds
        processed_events = 0

        with change_collection.watch(full_document="updateLookup") as stream:
            while time.time() < deadline and processed_events < settings.change_stream_max_events:
                event = stream.try_next()
                if event is None:
                    time.sleep(1)
                    continue

                processed_events += 1
                op_type = event.get("operationType")
                if op_type in {"insert", "update", "replace"}:
                    doc = event.get("fullDocument")
                    if doc:
                        normalized = normalize_mongo_document(doc)
                        doc_id = str(normalized.get("_id"))
                        upserts_by_id[doc_id] = normalized
                        deletes.discard(doc_id)
                        updated_epoch = normalized.get("updatedDate")
                        if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
                            max_updated_epoch = updated_epoch
                elif op_type == "delete":
                    doc_key = event.get("documentKey", {})
                    if "_id" in doc_key:
                        deletes.add(str(doc_key["_id"]))
                        upserts_by_id.pop(str(doc_key["_id"]), None)

        payload = {
            "upserts": list(upserts_by_id.values()),
            "deletes": list(deletes),
            "max_updated_epoch": max_updated_epoch,
            "initial_load": initial_load,
        }
        output_path = _write_changes(payload)

        _upsert_variable("customers_last_updated_epoch", str(max_updated_epoch))
        return output_path

    @task
    def load_to_oracle(changes_path: str) -> None:
        settings = get_settings()
        with open(changes_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)

        upserts = payload.get("upserts", [])
        deletes = payload.get("deletes", [])
        initial_load = payload.get("initial_load", False)

        if not upserts and not deletes:
            logger.info("No upserts/deletes found in %s", changes_path)
            return

        logger.info(
            "Loading to Oracle: dsn=%s user=%s upserts=%s deletes=%s initial_load=%s",
            settings.oracle_dsn,
            settings.oracle_user,
            len(upserts),
            len(deletes),
            initial_load,
        )

        conn = get_oracle_connection(settings)
        cursor = conn.cursor()

        for doc_id in deletes:
            cursor.execute("DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = :customer_id", {"customer_id": doc_id})

        for doc in upserts:
            customer_payload = build_customer_payload(doc)
            logger.info("Upserting customer_id=%s", customer_payload["customer"]["customer_id"])
            load_customer(cursor, customer_payload)
            conn.commit()

        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM CUSTOMERS")
        row_count = cursor.fetchone()[0]
        logger.info("CUSTOMERS count after load: %s", row_count)
        cursor.close()
        conn.close()

        if settings.initial_load and initial_load:
            _upsert_variable("customers_initial_load_done", "1")

    changes_path = extract_changes()
    load_to_oracle(changes_path)


mongo_oracle_etl()
