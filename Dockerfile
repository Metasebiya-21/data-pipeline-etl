ARG AIRFLOW_IMAGE_NAME=apache/airflow:2.9.3
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
