import json
import os
import time
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.session import create_session
from sqlalchemy.exc import IntegrityError

try:
    from sqlalchemy.dialects.postgresql import insert as pg_insert
except Exception:
    pg_insert = None
from pymongo import MongoClient

from src.mongo_utils import normalize_mongo_document, to_json_payload
from src.oracle_utils import get_oracle_connection
from src.settings import get_settings


def _parse_last_ts(last_ts_str: str, lookback_minutes: int) -> datetime:
    if last_ts_str:
        return datetime.fromisoformat(last_ts_str)
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


def _get_safe_run_id() -> str:
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
    return run_id.replace(":", "_").replace("/", "_")


def _write_changes(payload: dict) -> str:
    output_path = f"/opt/airflow/data/mongo_changes_{_get_safe_run_id()}.json"
    with open(output_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True)
    return output_path


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
    tags=["mongodb", "oracle", "incremental"],
)
def mongo_to_oracle_incremental():
    @task
    def extract_changes() -> str:
        settings = get_settings()
        client = MongoClient(settings.mongo_uri)
        db = client[settings.mongo_db]
        collection = db[settings.mongo_collection]

        last_ts_str = Variable.get("mongo_oracle_last_ts", default_var="")
        last_ts = _parse_last_ts(last_ts_str, settings.sync_lookback_minutes)

        upserts = []
        deletes = []
        max_updated_at = last_ts

        cursor = collection.find({"updated_at": {"$gte": last_ts}})
        for doc in cursor:
            normalized = normalize_mongo_document(doc)
            upserts.append(normalized)
            if "updated_at" in normalized:
                try:
                    doc_ts = datetime.fromisoformat(normalized["updated_at"])
                    if doc_ts > max_updated_at:
                        max_updated_at = doc_ts
                except ValueError:
                    pass

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
                        upserts.append(normalize_mongo_document(doc))
                elif op_type == "delete":
                    doc_key = event.get("documentKey", {})
                    if "_id" in doc_key:
                        deletes.append(str(doc_key["_id"]))

        payload = {"upserts": upserts, "deletes": deletes}
        output_path = _write_changes(payload)

        new_last_ts = max_updated_at
        _upsert_variable("mongo_oracle_last_ts", new_last_ts.isoformat())
        return output_path

    @task
    def load_to_oracle(changes_path: str) -> None:
        settings = get_settings()
        with open(changes_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)

        upserts = payload.get("upserts", [])
        deletes = payload.get("deletes", [])

        if not upserts and not deletes:
            return

        conn = get_oracle_connection(settings)
        cursor = conn.cursor()

        merge_sql = f"""
            MERGE INTO {settings.oracle_table} t
            USING (
                SELECT :id AS id, :doc_json AS doc_json, :updated_at AS updated_at
                FROM dual
            ) s
            ON (t.id = s.id)
            WHEN MATCHED THEN UPDATE
              SET t.doc_json = s.doc_json, t.updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (id, doc_json, updated_at)
              VALUES (s.id, s.doc_json, s.updated_at)
        """

        for doc in upserts:
            doc_id = doc.get("_id")
            updated_at = doc.get("updated_at")
            cursor.execute(
                merge_sql,
                {
                    "id": doc_id,
                    "doc_json": to_json_payload(doc),
                    "updated_at": updated_at,
                },
            )

        if deletes:
            delete_sql = f"DELETE FROM {settings.oracle_table} WHERE id = :id"
            for doc_id in deletes:
                cursor.execute(delete_sql, {"id": doc_id})

        conn.commit()
        cursor.close()
        conn.close()

    changes_path = extract_changes()
    load_to_oracle(changes_path)


mongo_to_oracle_incremental()
