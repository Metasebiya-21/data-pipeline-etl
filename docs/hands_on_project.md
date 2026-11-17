# Hands-on project: mini MDM on Snowflake

Load two MongoDB test databases into Snowflake, model them with dbt, and link the same customer across both sources with a Snowpark Python model.

```
MongoDB  coop-customer-management-db (MICHU, ~88K)   wegagen-customer-management-db (Efoyta, ~100K)
   │  scripts/initial_load.py: NDJSON files → PUT @RAW.CUSTOMERS_STAGE → COPY INTO RAW.CUSTOMERS_RAW
   ▼
RAW.CUSTOMERS_RAW (VARIANT + source_database)
   │  dbt build
   ▼
STAGING.stg_customer_events → INTERMEDIATE.int_customers_latest (incremental MERGE on record_key)
   ├─► MARTS: dim_customer, customer_addresses, customer_bank_accounts, dim_business
   ├─► SNAPSHOTS: customer_address_snapshot (SCD2)
   └─► LINKAGE: int_records_canonical → int_candidate_pairs (blocking)
                → match_scores (Snowpark Python model) → master_customer_map
```

## Step 0: prerequisites (on your Mac)
```bash
cd data-pipeline-etl && git checkout feature/snowflake_dbt_elt
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements-local.txt
```

## Step 1: Snowflake trial and setup
1. Sign up at signup.snowflake.com: **Enterprise** edition, AWS, a nearby region.
2. In Snowsight, open a SQL worksheet and run all of `snowflake/01_setup.sql`.
3. Give the roles to your own user (replace `<YOUR_USER>`):
   ```sql
   USE ROLE SECURITYADMIN;
   GRANT ROLE LOADER TO USER <YOUR_USER>;
   GRANT ROLE TRANSFORMER TO USER <YOUR_USER>;
   ```
4. Python models need the Anaconda terms accepted once: **Admin → Billing & Terms → Anaconda → Enable** (as ORGADMIN/ACCOUNTADMIN).

## Step 2: configure `.env`
```bash
cp .env.example .env     # then edit it
```
Fill in `MONGO_URI` (your local replica set), `SNOWFLAKE_ACCOUNT` (format `orgname-accountname`, from Snowsight → account menu), `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`. `.env` is git-ignored: never commit it.

## Step 3: load MongoDB into Snowflake (EL)
```bash
set -a; source .env; set +a
python -m scripts.initial_load
```
It loads both databases in files of 25,000 documents, then prints a row count per source. **Check in Snowflake:**
```sql
USE ROLE LOADER; USE WAREHOUSE LOAD_WH;
LIST @LENDING.RAW.CUSTOMERS_STAGE;                                   -- the staged .gz files
SELECT source_database, COUNT(*) FROM LENDING.RAW.CUSTOMERS_RAW GROUP BY 1;
SELECT doc:fullName::string, doc:personalAddresses[0]:region::string
FROM LENDING.RAW.CUSTOMERS_RAW LIMIT 10;                             -- reading VARIANT
```
**Experiment: idempotency.** Run `python -m scripts.initial_load` again. New files get new names, so they **will** load again. Then run the `COPY INTO ... FILES=(...)` from `COPY_HISTORY` for a file that was already loaded: Snowflake reports `LOAD_SKIPPED`/0 rows.

## Step 4: transform with dbt (T)
```bash
cd dbt
dbt debug --profiles-dir .                 # connection OK?
dbt build --profiles-dir . --target dev    # seeds, models, snapshot, tests → schemas dev_<you>_*
```
**Look at:**
- `target/compiled/lending_dw/models/...`: the real SQL dbt sent.
- In Snowsight **Query History** (filter query tag `dbt_lending_dw`): the `MERGE` dbt generated for `int_customers_latest`.
- `dbt docs generate --profiles-dir . && dbt docs serve`: the lineage graph.

## Step 5: look at the linkage results
```sql
-- how many pairs did blocking produce, by rule?
SELECT blocking_rules, COUNT(*) FROM <schema>_linkage.int_candidate_pairs GROUP BY 1 ORDER BY 2 DESC;

-- score distribution
SELECT ROUND(match_score, 1) AS score, COUNT(*) FROM <schema>_linkage.match_scores GROUP BY 1 ORDER BY 1;

-- customers present in BOTH banks
SELECT COUNT(DISTINCT master_key) FROM <schema>_linkage.master_customer_map WHERE matched_records > 0;
```
Look at a few pairs around 0.7–0.85 by hand. Are they really the same person? That's how you tune the weights and threshold.

## Step 6: exercises (each maps to an interview topic)
| # | Exercise | Concept |
|---|---|---|
| 1 | Run `dbt build` again with no new data and check `int_customers_latest` in Query History: how many rows did the `MERGE` touch? | Incremental models |
| 2 | Update one customer's address in MongoDB, load it, run `dbt build` and `dbt snapshot` again, then query `customer_address_snapshot` for that `record_key` | SCD Type 2 |
| 3 | `DELETE FROM <schema>_marts.dim_customer WHERE source_database = 'michu_coop';` then restore it with `AT(OFFSET => -60*5)` or `BEFORE(STATEMENT => ...)` | Time Travel |
| 4 | `CREATE DATABASE LENDING_DEV CLONE LENDING;` and check storage | Zero-copy cloning |
| 5 | Create a stream on `RAW.CUSTOMERS_RAW`, load more data, `SELECT * FROM` the stream, then consume it with an `INSERT ... SELECT` | Streams |
| 6 | Rewrite `int_records_canonical` as a `CREATE DYNAMIC TABLE ... TARGET_LAG = '15 minutes'` | Dynamic tables |
| 7 | Open the Query Profile of a query filtering `_loaded_at` vs. one filtering `customer_id`: compare partitions scanned vs. total | Pruning |
| 8 | Run the same dashboard-style query twice and compare the second run's time | Result cache |
| 9 | Replace the weighted score in `match_scores.py` with an XGBoost model trained on labelled pairs (Snowpark ML) | Snowpark ML |
| 10 | Replace the one-hop `master_customer_map` with real clusters (connected components), e.g. a recursive CTE or `networkx` in a Python model | MDM clustering |

## Costs
Everything uses X-Small warehouses with 60-second auto-suspend, plus a resource monitor (`ELT_MONITOR`, 20 credits). This project uses a small fraction of the trial's free credits. Suspend anything you leave running.
