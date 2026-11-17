ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# dbt in its own virtualenv so its dependencies never conflict with Airflow's
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.11.0" "dbt-snowflake~=1.11.0"
