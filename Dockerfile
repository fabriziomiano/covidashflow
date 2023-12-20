
FROM apache/airflow:2.8.0
ADD requirements.txt .
ADD .env .
RUN pip install --upgrade pip && \
    pip install apache-airflow==${AIRFLOW_VERSION} && \
    pip install -r requirements.txt
