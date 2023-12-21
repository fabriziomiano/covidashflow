FROM apache/airflow:2.8.0
COPY requirements.txt /
RUN pip install --upgrade pip && \
    pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt