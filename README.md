# MongoDB to Oracle Airflow Pipeline

This project scaffolds an Apache Airflow (Docker) pipeline that incrementally syncs MongoDB documents to Oracle and listens briefly to change streams for update/delete events.

## What is included
- Airflow Docker Compose stack (webserver, scheduler, metadata DB)
- Incremental extract + change stream capture
- Oracle upsert + delete logic
- Pydantic settings loading from `.env`

## Prerequisites
- Docker Desktop
- MongoDB replica set (required for change streams)
- Oracle database reachable from the Airflow containers

## Quick start
1. Update `.env` with your MongoDB and Oracle settings.
2. Start Airflow:

```bash
docker compose up airflow-init
docker compose up -d
```

3. Open Airflow at `http://localhost:8080` and enable the DAG `mongo_to_oracle_incremental`.

## Oracle table expectation
Create a target table with an ID, JSON payload, and updated timestamp:

```sql
CREATE TABLE MONGO_ITEMS (
  ID VARCHAR2(64) PRIMARY KEY,
  DOC_JSON CLOB,
  UPDATED_AT VARCHAR2(64)
);
```

## Oracle container
The compose file includes an Oracle Free container. Default DSN for the Airflow container is `oracle:1521/FREEPDB1`.
If you want to use an external Oracle DB later, update `ORACLE_DSN` (or use `ORACLE_DSN_EXTERNAL` as a reference).

## Report ETL schema
The Oracle init script creates a `REPORT_ETL` schema and applies `db/report_etl_schema.sql` automatically on first start.
The `mongo_oracle_etl` DAG loads from MongoDB database `coop-customer-management-db` and collection `customers`.
Set `FULL_REFRESH=1` in `.env` to load all customer documents regardless of `updatedDate`/`createdDate`.
Set `INITIAL_LOAD=1` for a one-time full load; the DAG will mark it complete via `customers_initial_load_done`.

## Notes
- Incremental extraction uses `updated_at >= last_ts`. The DAG stores `last_ts` in Airflow Variables as `mongo_oracle_last_ts`.
- Change stream listening is bounded by `CHANGE_STREAM_MAX_SECONDS` and `CHANGE_STREAM_MAX_EVENTS`.
- Update the DAG schedule or table mapping as needed.
