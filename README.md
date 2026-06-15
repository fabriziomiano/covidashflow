# COVIDashFlow

Rewritten Airflow ETL project for the COVIDash.it data backend.

The rewrite keeps the original pipelines and MongoDB collection contracts:

- PCM-DPC national, regional, and provincial raw data
- PCM-DPC trend cards
- PCM-DPC chart series
- PCM-DPC regional and provincial breakdowns
- Italia Open Data vaccination administrations
- Italia Open Data vaccination summaries
- Italia Open Data population data

## What changed

The original project mixed Airflow, MongoDB lookups, extraction, transformation, and loading at module import time. This version keeps the same ETL logic but packages it under `src/covidashflow` so transformations can be tested without an Airflow runtime.

Airflow DAG files in `dags/` are now thin wrappers around package functions.

## Run locally

Create an environment file:

```shell
cp .env.example .env
```

The default `.env.example` starts a local MongoDB container and points Airflow to
the `covidash` database. To use an external MongoDB instance, change
`AIRFLOW_CONN_MONGO_DEFAULT`.

Run the stack:

```shell
docker compose --env-file .env up --build
```

The Airflow UI listens on `http://0.0.0.0:8080`.
The local login is `airflow` / `airflow`.

If another Airflow instance already uses port `8080`, set `AIRFLOW_WEBSERVER_PORT=8081`
in `.env` before starting the stack.

Stop the stack with:

```shell
docker compose --env-file .env down
```

## Test

```shell
python -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
pytest
```

You can also validate the Dockerized Airflow DAG parse:

```shell
docker compose --env-file .env run --rm airflow-cli dags list --subdir /opt/airflow/dags
```
