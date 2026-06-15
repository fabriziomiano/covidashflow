FROM apache/airflow:2.8.0

COPY --chown=airflow:0 pyproject.toml README.md /opt/covidashflow/
COPY --chown=airflow:0 src /opt/covidashflow/src
COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip && \
    pip install --no-cache-dir /opt/covidashflow
