# mongo_oracle_etl DAG — Detailed Walkthrough

This document explains `dags/mongo_oracle_etl.py` line by line. The goal of the DAG is to:
- Extract new/changed documents from MongoDB (`customers` collection),
- Convert them into a relational payload,
- Load them into Oracle,
- Track progress so future runs are incremental.

## Source file
`dags/mongo_oracle_etl.py`

## Full code with line-by-line notes

```python
 1 | import json
 2 | import logging
 3 | import os
 4 | import time
 5 | from datetime import datetime, timedelta, timezone
 6 |
 7 | from airflow.decorators import dag, task
 8 | from airflow.models import Variable
 9 | from airflow.utils.session import create_session
10 | from pymongo import MongoClient
11 | from sqlalchemy.exc import IntegrityError
12 |
13 | from src.customer_etl import build_customer_payload, load_customer
14 | from src.mongo_utils import normalize_mongo_document
15 | from src.oracle_utils import get_oracle_connection
16 | from src.settings import get_settings
17 |
18 | logger = logging.getLogger(__name__)
19 |
20 | try:
21 |     from sqlalchemy.dialects.postgresql import insert as pg_insert
22 | except Exception:
23 |     pg_insert = None
```

- Lines 1–5: Standard library imports for JSON, logging, environment access, time math, and UTC timestamps.
- Lines 7–9: Airflow utilities: DAG/task decorators and access to Airflow Variables with DB sessions.
- Line 10: MongoDB client.
- Line 11: SQLAlchemy error used for safe “upsert” on Airflow Variables.
- Lines 13–16: Internal project utilities for transforming Mongo documents, Oracle connectivity, and settings.
- Line 18: Module logger for structured DAG logs.
- Lines 20–23: Optional PostgreSQL upsert helper. If Airflow metadata DB is PostgreSQL, we can do `ON CONFLICT`.

```python
24 | def _epoch_now() -> int:
25 |     return int(datetime.now(timezone.utc).timestamp())
```

- Lines 24–25: Returns the current UTC time as epoch seconds.

```python
27 | def _parse_last_epoch(last_epoch_str: str, lookback_minutes: int) -> int:
28 |     if last_epoch_str:
29 |         try:
30 |             return int(last_epoch_str)
31 |         except ValueError:
32 |             return _epoch_now() - (lookback_minutes * 60)
33 |     return _epoch_now() - (lookback_minutes * 60)
```

- Lines 27–33: Parses the “last processed timestamp” from Airflow Variables.
- If missing or invalid, it falls back to “now minus lookback window.”

```python
36 | def _get_safe_run_id() -> str:
37 |     run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual")
38 |     return run_id.replace(":", "_").replace("/", "_")
```

- Lines 36–38: Builds a filesystem-safe run ID for naming JSON output files.

```python
41 | def _write_changes(payload: dict) -> str:
42 |     output_path = f"/opt/airflow/data/mongo_oracle_{_get_safe_run_id()}.json"
43 |     with open(output_path, "w", encoding="utf-8") as handle:
44 |         json.dump(payload, handle, ensure_ascii=True)
45 |     return output_path
```

- Lines 41–45: Serializes extracted changes to a JSON file inside `/opt/airflow/data` and returns the path.

```python
48 | def _initial_load_enabled(settings) -> bool:
49 |     if not settings.initial_load:
50 |         return False
51 |     done_flag = Variable.get("customers_initial_load_done", default_var="0")
52 |     return done_flag != "1"
```

- Lines 48–52: Implements one-time initial load.
- If `INITIAL_LOAD=1` and the `customers_initial_load_done` flag is not set, the DAG will full-load once.

```python
55 | def _upsert_variable(key: str, value: str) -> None:
56 |     with create_session() as session:
57 |         dialect = session.get_bind().dialect.name
58 |         if pg_insert and dialect == "postgresql":
59 |             stmt = pg_insert(Variable).values(
60 |                 key=key,
61 |                 val=value,
62 |                 description=None,
63 |                 is_encrypted=False,
64 |             )
65 |             stmt = stmt.on_conflict_do_update(
66 |                 index_elements=["key"],
67 |                 set_={
68 |                     "val": value,
69 |                     "description": None,
70 |                     "is_encrypted": False,
71 |                 },
72 |             )
73 |             session.execute(stmt)
74 |             session.flush()
75 |             return
76 |
77 |         try:
78 |             Variable.set(key, value, session=session)
79 |         except IntegrityError:
80 |             session.rollback()
81 |             session.query(Variable).filter(Variable.key == key).update(
82 |                 {
83 |                     "val": value,
84 |                     "description": None,
85 |                     "is_encrypted": False,
86 |                 }
87 |             )
88 |             session.flush()
```

- Lines 55–88: Concurrency‑safe upsert for Airflow Variables.
- PostgreSQL: uses `INSERT ... ON CONFLICT`.
- Other DBs: tries to insert, and if it collides, updates.

```python
91 | @dag(
92 |     schedule="@hourly",
93 |     start_date=datetime(2024, 1, 1),
94 |     catchup=False,
95 |     tags=["mongodb", "oracle", "customers", "etl"],
96 | )
97 | def mongo_oracle_etl():
```

- Lines 91–97: DAG definition. Runs hourly and does not backfill old dates.

### Task: extract_changes

```python
100|     @task
101|     def extract_changes() -> str:
102|         settings = get_settings()
103|         client = MongoClient(settings.mongo_uri)
104|         db = client[settings.mongo_db]
105|         collection = db[settings.mongo_collection]
```

- Lines 100–105: Load settings and connect to the MongoDB collection.

```python
107|         last_epoch_str = Variable.get("customers_last_updated_epoch", default_var="")
108|         last_epoch = _parse_last_epoch(last_epoch_str, settings.sync_lookback_minutes)
```

- Lines 107–108: Read “last updated” cursor from Airflow Variables.

```python
110|         upserts_by_id = {}
111|         deletes = set()
112|         max_updated_epoch = last_epoch
```

- Lines 110–112: Containers for upserts, deletes, and the max updated time we see.

```python
114|         initial_load = _initial_load_enabled(settings)
115|         if settings.full_refresh or initial_load:
116|             cursor = collection.find({})
117|         else:
118|             cursor = collection.find(
119|                 {
120|                     "$or": [
121|                         {"updatedDate": {"$gte": last_epoch}},
122|                         {"updatedDate": {"$exists": False}, "createdDate": {"$gte": last_epoch}},
123|                     ]
124|                 }
125|             )
```

- Lines 114–125: Chooses full or incremental extraction.
- Full: fetch all docs.
- Incremental: fetch docs updated since last epoch; if `updatedDate` is missing, fallback to `createdDate`.

```python
126|         for doc in cursor:
127|             normalized = normalize_mongo_document(doc)
128|             doc_id = str(normalized.get("_id"))
129|             upserts_by_id[doc_id] = normalized
130|             deletes.discard(doc_id)
131|             updated_epoch = normalized.get("updatedDate")
132|             if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
133|                 max_updated_epoch = updated_epoch
```

- Lines 126–133: Normalize each doc, register as upsert, and track max `updatedDate`.

```python
135|         change_db = client[settings.mongo_change_stream_db]
136|         change_collection = change_db[settings.mongo_change_stream_collection]
137|         deadline = time.time() + settings.change_stream_max_seconds
138|         processed_events = 0
```

- Lines 135–138: Setup for change stream listening.

```python
140|         with change_collection.watch(full_document="updateLookup") as stream:
141|             while time.time() < deadline and processed_events < settings.change_stream_max_events:
142|                 event = stream.try_next()
143|                 if event is None:
144|                     time.sleep(1)
145|                     continue
```

- Lines 140–145: Poll the change stream for a bounded time or number of events.

```python
147|                 processed_events += 1
148|                 op_type = event.get("operationType")
149|                 if op_type in {"insert", "update", "replace"}:
150|                     doc = event.get("fullDocument")
151|                     if doc:
152|                         normalized = normalize_mongo_document(doc)
153|                         doc_id = str(normalized.get("_id"))
154|                         upserts_by_id[doc_id] = normalized
155|                         deletes.discard(doc_id)
156|                         updated_epoch = normalized.get("updatedDate")
157|                         if isinstance(updated_epoch, int) and updated_epoch > max_updated_epoch:
158|                             max_updated_epoch = updated_epoch
159|                 elif op_type == "delete":
160|                     doc_key = event.get("documentKey", {})
161|                     if "_id" in doc_key:
162|                         deletes.add(str(doc_key["_id"]))
163|                         upserts_by_id.pop(str(doc_key["_id"]), None)
```

- Lines 147–163: Update in-memory upsert/delete sets based on change stream events.

```python
165|         payload = {
166|             "upserts": list(upserts_by_id.values()),
167|             "deletes": list(deletes),
168|             "max_updated_epoch": max_updated_epoch,
169|             "initial_load": initial_load,
170|         }
171|         output_path = _write_changes(payload)
```

- Lines 165–171: Build the payload and write it to disk.

```python
173|         _upsert_variable("customers_last_updated_epoch", str(max_updated_epoch))
174|         return output_path
```

- Lines 173–174: Persist the cursor for next run and return the payload path.

### Task: load_to_oracle

```python
177|     @task
178|     def load_to_oracle(changes_path: str) -> None:
179|         settings = get_settings()
180|         with open(changes_path, "r", encoding="utf-8") as handle:
181|             payload = json.load(handle)
```

- Lines 177–181: Load the changes file written by `extract_changes`.

```python
183|         upserts = payload.get("upserts", [])
184|         deletes = payload.get("deletes", [])
185|         initial_load = payload.get("initial_load", False)
```

- Lines 183–185: Extract operation lists from the payload.

```python
187|         if not upserts and not deletes:
188|             logger.info("No upserts/deletes found in %s", changes_path)
189|             return
```

- Lines 187–189: Skip work if there is nothing to apply.

```python
191|         logger.info(
192|             "Loading to Oracle: dsn=%s user=%s upserts=%s deletes=%s initial_load=%s",
193|             settings.oracle_dsn,
194|             settings.oracle_user,
195|             len(upserts),
196|             len(deletes),
197|             initial_load,
198|         )
```

- Lines 191–198: Log connection context and counts.

```python
200|         conn = get_oracle_connection(settings)
201|         cursor = conn.cursor()
```

- Lines 200–201: Connect to Oracle and open a cursor.

```python
203|         for doc_id in deletes:
204|             cursor.execute("DELETE FROM CUSTOMERS WHERE CUSTOMER_ID = :customer_id", {"customer_id": doc_id})
```

- Lines 203–204: Delete customer rows; foreign keys with `ON DELETE CASCADE` remove related records.

```python
206|         for doc in upserts:
207|             customer_payload = build_customer_payload(doc)
208|             logger.info("Upserting customer_id=%s", customer_payload["customer"]["customer_id"])
209|             load_customer(cursor, customer_payload)
210|             conn.commit()
```

- Lines 206–210: Transform each Mongo doc into relational rows and apply them to Oracle.
- `load_customer()` handles all related tables.

```python
212|         conn.commit()
213|         cursor.execute("SELECT COUNT(*) FROM CUSTOMERS")
214|         row_count = cursor.fetchone()[0]
215|         logger.info("CUSTOMERS count after load: %s", row_count)
216|         cursor.close()
217|         conn.close()
```

- Lines 212–217: Final commit, quick row count log, and close connection.

```python
219|         if settings.initial_load and initial_load:
220|             _upsert_variable("customers_initial_load_done", "1")
```

- Lines 219–220: Mark initial load as done so it only runs once.

```python
222|     changes_path = extract_changes()
223|     load_to_oracle(changes_path)
```

- Lines 222–223: Task dependency—`load_to_oracle` runs after `extract_changes` and receives its output path.

```python
226| mongo_oracle_etl()
```

- Line 226: Instantiates the DAG when the file is imported by Airflow.

## Notes on related modules

- `src/customer_etl.py`: Converts a Mongo customer document into rows for all Oracle tables.
- `src/settings.py`: Loads environment variables and toggles such as `FULL_REFRESH` and `INITIAL_LOAD`.
- `src/oracle_utils.py`: Connects using the `oracledb` driver and DSN from `.env`.

## Common runtime switches

- `FULL_REFRESH=1`: Pull all documents every run.
- `INITIAL_LOAD=1`: Full load once, then incremental thereafter.
- `SYNC_LOOKBACK_MINUTES`: How far back to look when the last cursor is missing/invalid.
