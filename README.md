# MongoDB to Oracle Airflow ETL

This project runs an Apache Airflow pipeline (Docker) that syncs MongoDB customer documents into an Oracle relational schema. It supports incremental loads, short change stream polling, and one-time full loads.

## What is included
- Docker Compose stack for Airflow + Postgres metadata DB + Oracle Free
- Incremental MongoDB extraction with change streams
- Full relational load into Oracle `REPORT_ETL` schema
- Pydantic settings loaded from `.env`
- Detailed docs in `docs/`

## Prerequisites
- Docker
- MongoDB replica set (required for change streams)
- Oracle reachable from the Airflow containers (local container included)
- All containers on the same Docker network

## Project layout
- `dags/mongo_oracle_etl.py`: main ETL DAG
- `src/customer_etl.py`: transforms Mongo docs into Oracle rows
- `db/report_etl_schema.sql`: Oracle schema definition
- `db/oracle-init/01_create_report_etl.sh`: Oracle init script
- `docs/mongo_oracle_etl.md`: DAG walkthrough
- `docs/customer_etl.md`: mapping walkthrough

## Quick start
1. Update `.env` with MongoDB and Oracle settings.
2. Build and start services:

```bash
docker compose up --build -d
```

3. Open Airflow at `http://localhost:8080` and enable the DAG `mongo_oracle_etl`.

## Oracle setup

### Oracle container
The compose file includes an Oracle Free container. Default DSN for the Airflow container is:

```
oracle:1521/FREEPDB1
```

If you want to use an external Oracle DB later, update `ORACLE_DSN` (or use `ORACLE_DSN_EXTERNAL` as a reference).

### REPORT_ETL schema
On first start, the init script creates a `REPORT_ETL` schema and applies `db/report_etl_schema.sql` automatically.

Manual run (inside the Oracle container):

```bash
docker exec -it airflow_etl_oracle bash
bash -x /container-entrypoint-initdb.d/01_create_report_etl.sh
```

## MongoDB source
The DAG loads from:
- Database: `coop-customer-management-db`
- Collection: `customers`

These values are controlled by `MONGO_DB` and `MONGO_COLLECTION` in `.env`.

## ETL behavior

### Incremental extraction
- Uses `updatedDate >= last_epoch`
- If `updatedDate` is missing, falls back to `createdDate >= last_epoch`
- A short change stream poll captures inserts/updates/deletes during the run
- The cursor is stored in Airflow Variables as `customers_last_updated_epoch`

### Full refresh
Set `FULL_REFRESH=1` in `.env` to load all documents every run.

### One-time initial load
Set `INITIAL_LOAD=1` in `.env` to load all documents once. The DAG will set:

```
customers_initial_load_done=1
```

After that, it returns to incremental mode.

## Verification

Connect to Oracle (from host):

```bash
sqlplus report_etl/report_etl_password@localhost:1521/FREEPDB1
```

Check row counts:

```sql
SELECT COUNT(*) FROM CUSTOMERS;
SELECT COUNT(*) FROM CUSTOMER_PERSONAL_ADDRESSES;
SELECT COUNT(*) FROM CUSTOMER_DOCUMENTS;
```

## Troubleshooting
- `ORA-12514`: wrong service name; use `FREEPDB1`.
- `ORA-01017`: user not created; recreate Oracle volume or create user manually.
- Empty loads: reset `customers_last_updated_epoch` or set `INITIAL_LOAD=1`.

## Notes
- Change stream listening is bounded by `CHANGE_STREAM_MAX_SECONDS` and `CHANGE_STREAM_MAX_EVENTS`.
- Update the DAG schedule or mapping logic as needed.
