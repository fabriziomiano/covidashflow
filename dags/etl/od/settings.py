from airflow.models import Variable

DAG_ID = Variable.get("OD_DAG_ID")
SCHEDULE_INTERVAL = Variable.get("OD_SCHEDULE_INTERVAL")
