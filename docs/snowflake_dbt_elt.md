# MongoDB → Snowflake ELT with dbt: walkthrough

The same customer data as the Oracle DAG, but loaded **ELT-style** into Snowflake: land the raw documents first, then transform them inside Snowflake with dbt.

```
MongoDB (one database per source: michu_coop, efoyta_wegagen, ...)
  │  E  extract_changes[source]   one mapped task per source (dynamic task mapping):
  │                               find(updatedDate >= watermark) + change stream (resume token)
  ▼
NDJSON file of change events  (/opt/airflow/data/customers_<run_id>.ndjson)
  │  L  load_to_snowflake[source]  PUT → @RAW.CUSTOMERS_STAGE → COPY INTO RAW.CUSTOMERS_RAW   (LOAD_WH)
  │     commit_state[source]       save that source's watermark + resume token only after its load succeeded
  ▼
RAW.CUSTOMERS_RAW  (one row per event, full document as VARIANT, transient table)
  │  T  dbt build                                                                       (TRANSFORM_WH)
  ▼
STAGING.stg_customer_events      view: typed fields pulled out of the VARIANT
INTERMEDIATE.int_customers_latest incremental MERGE: latest state per customer (+ is_deleted)
MARTS.dim_customer               SCD1 current customers
MARTS.customer_addresses         LATERAL FLATTEN of personalAddresses + region reference data
MARTS.customer_bank_accounts     LATERAL FLATTEN of bankInformations
MARTS.dim_business               LATERAL FLATTEN of businesses
SNAPSHOTS.customer_address_snapshot  SCD2 history of each customer's primary address
  │
  ▼
Power BI on BI_WH (REPORTER role)
```

## Files
| File | Purpose |
|---|---|
| `snowflake/01_setup.sql` | Warehouses per workload, database, stage, file format, raw table, roles, resource monitor |
| `snowflake/02_snowpipe_s3.sql` | Optional: S3 external stage + Snowpipe auto-ingest instead of the Airflow load task |
| `dags/mongo_snowflake_elt.py` | The Airflow DAG |
| `src/snowflake_utils.py` | Connection, `PUT` to stage, `COPY INTO` raw |
| `dbt/` | dbt project: models, seed, snapshot, tests |

## Snowflake concepts, mapped to the code

**Separate warehouses per workload** (`01_setup.sql`): Airflow loads on `LOAD_WH`, dbt transforms on `TRANSFORM_WH`, Power BI reads on `BI_WH`. Same data, separate compute, so a heavy dbt run never slows dashboards. Each has `AUTO_SUSPEND` so idle time costs nothing.

**Stage** (`CUSTOMERS_STAGE`): a named internal stage where the NDJSON file waits before loading. `PUT` uploads it (gzip-compressed). Files stay in the stage, so a raw load can be replayed.

**`COPY INTO` is idempotent** (`snowflake_utils.copy_file_into_raw`): Snowflake remembers loaded files for 64 days, so an Airflow retry of the same file loads nothing twice. `METADATA$FILENAME` and `METADATA$FILE_ROW_NUMBER` are stored on every row for lineage and to trace rejected records.

**VARIANT + `LATERAL FLATTEN`** (dbt marts): the whole MongoDB document lands as VARIANT. Nested arrays (`personalAddresses`, `bankInformations`, `businesses`) become rows with `LATERAL FLATTEN` in SQL. That replaces the Python flattening in `customer_etl.py`.

**Transient raw table:** no Fail-safe and 1 day of Time Travel, which is cheaper. The raw table can always be reloaded from the stage or from MongoDB.

**Snowpipe (optional):** with `02_snowpipe_s3.sql`, Airflow only writes files to S3, and S3 event notifications trigger the pipe, which runs the same `COPY` on serverless compute.

## dbt concepts, mapped to the code

**Layers:** `staging` (views: rename and type), `intermediate` (incremental), `marts` (tables for BI). Like the zoomcamp `taxi_rides_ny` project.

**Incremental MERGE with a lookback** (`int_customers_latest`): each run reads only events loaded in the last run (plus a 1-hour lookback) and merges on `customer_id`. Reprocessing an event is harmless because the merge is keyed.

**Soft deletes:** a MongoDB delete arrives as a `delete` event. The latest state keeps the row with `is_deleted = true`, and marts filter it out. Nothing is physically lost, so the history stays auditable.

**SCD Type 1 vs Type 2:** `dim_customer` keeps current values only (Type 1). `customer_address_snapshot` is a dbt snapshot (`strategy='check'`) that keeps every version of a customer's primary address with `dbt_valid_from`/`dbt_valid_to` (Type 2), so you can ask where a customer lived when a loan was issued. `invalidate_hard_deletes` closes the row when the customer is deleted.

**Reference data (MDM):** `seeds/region_codes.csv` holds the official region codes. `customer_addresses` joins to it, and a `relationships` test (severity `warn`) flags regions that don't match a known code.

**Tests:** `unique`/`not_null` on keys, `relationships` between marts, `accepted_values` on the raw change type, source **freshness** on `_loaded_at` (warn after 2 h, error after 6 h), and a singular reconciliation test (`tests/assert_dim_customer_matches_latest.sql`).

**Environments:** `generate_schema_name` writes to `STAGING`/`MARTS` in prod, and to `dev_<user>_staging` in dev, so developers never overwrite production. Credentials come from environment variables in `profiles.yml`.

## Several sources: dynamic task mapping
`MONGO_SOURCES` lists the source databases (`mongo_db=label,...`). `get_sources` returns that list and `extract_changes.expand(source=...)` creates **one task instance per source** at runtime; `load_to_snowflake` and `commit_state` are mapped over the previous task's results, so each source flows through its own chain. In the Airflow UI each instance is labelled with the source name (`map_index_template`).

- **State per source:** watermark, resume token and initial-load flag are Airflow Variables named `sf_<name>__<label>`, so one source failing never moves another source's state.
- **Adding a source** is a config change (append to `MONGO_SOURCES`), not a code change.
- **`dbt_build`** waits for all sources and uses `trigger_rule="all_done"`: if one source fails this hour, the others are still transformed; dbt's incremental models pick up the failed source on its next successful run.

## Reliability fixes compared with the Oracle DAG
1. **Watermark saved after the load, not before.** `commit_state` runs only if `load_to_snowflake` succeeded. If the load fails, the next run re-extracts the same window instead of skipping it.
2. **Change stream resume token stored.** Deletes that happen between hourly runs are captured. If the token has fallen out of the oplog window, the DAG logs a warning and starts a new stream, and a `FULL_REFRESH=1` run reconciles.
3. **`max_active_runs=1`:** two runs never `COPY`/`MERGE` into the same tables at the same time.

## Hands-on project
To load two MongoDB sources and link customers across them (dbt + Snowpark), follow `docs/hands_on_project.md`.

## Run it
1. Run `snowflake/01_setup.sql` in a Snowsight worksheet.
2. `cp .env.example .env` and fill in MongoDB and Snowflake values.
3. `docker compose up --build -d`, then enable `mongo_snowflake_elt` in Airflow.
4. Develop dbt locally: `cd dbt && dbt build --profiles-dir . --target dev`.

## Interview summary
> "I extract changed MongoDB documents incrementally with a watermark and a change stream resume token, write them as NDJSON change events, `PUT` them to a named stage and `COPY INTO` a transient raw table as VARIANT. `COPY` is idempotent, so retries are safe, and the watermark only advances after a successful load. dbt then builds staging views, an incremental `MERGE` for the latest state per customer with soft deletes, flattened marts for Power BI, an SCD2 snapshot of addresses, and tests including source freshness and reconciliation. Loading, transforming and BI each run on their own warehouse."
