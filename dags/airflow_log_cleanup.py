"""Airflow maintenance DAG that removes old local task log files."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "airflow-log-cleanup"
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")
SCHEDULE_INTERVAL = "@weekly"
DAG_OWNER_NAME = "operations"
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days",
    default_var=30,
)
ENABLE_DELETE_CHILD_LOG = Variable.get(
    "airflow_log_cleanup__enable_delete_child_log",
    default_var="True",
)
LOG_CLEANUP_PROCESS_LOCK_FILE = "/tmp/airflow_log_cleanup_worker.lock"
max_log_age_template = (
    "{{ dag_run.conf.get('maxLogAgeInDays', "
    f"{DEFAULT_MAX_LOG_AGE_IN_DAYS}) if dag_run else {DEFAULT_MAX_LOG_AGE_IN_DAYS} }}"
)

try:
    BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER").rstrip("/")
except Exception:
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")

DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        child_log_directory = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY")
        if child_log_directory.strip():
            DIRECTORIES_TO_DELETE.append(child_log_directory)
    except Exception:
        pass

if not BASE_LOG_FOLDER:
    raise ValueError("Airflow BASE_LOG_FOLDER is empty.")

default_args = {
    "owner": DAG_OWNER_NAME,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

log_cleanup_command = f"""
set -e

BASE_LOG_FOLDER="{{{{ params.directory }}}}"
WORKER_SLEEP_TIME="{{{{ params.sleep_time }}}}"
MAX_LOG_AGE_IN_DAYS="{max_log_age_template}"

sleep "${{WORKER_SLEEP_TIME}}s"

echo "BASE_LOG_FOLDER: ${{BASE_LOG_FOLDER}}"
echo "MAX_LOG_AGE_IN_DAYS: ${{MAX_LOG_AGE_IN_DAYS}}"

if [ -f "{LOG_CLEANUP_PROCESS_LOCK_FILE}" ]; then
  echo "Another cleanup task is already running on this worker."
  exit 0
fi

touch "{LOG_CLEANUP_PROCESS_LOCK_FILE}"
trap 'rm -f "{LOG_CLEANUP_PROCESS_LOCK_FILE}"' EXIT

find "${{BASE_LOG_FOLDER}}"/*/* -type f -mtime +"${{MAX_LOG_AGE_IN_DAYS}}" -print -delete || true
find "${{BASE_LOG_FOLDER}}"/*/* -type d -empty -print -delete || true
find "${{BASE_LOG_FOLDER}}"/* -type d -empty -print -delete || true
"""

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=False,
    tags=["airflow-maintenance"],
) as dag:
    start = EmptyOperator(task_id="start")

    for directory_index, directory in enumerate(DIRECTORIES_TO_DELETE):
        cleanup = BashOperator(
            task_id=f"log_cleanup_dir_{directory_index}",
            bash_command=log_cleanup_command,
            params={"directory": directory, "sleep_time": 3},
        )
        start >> cleanup
