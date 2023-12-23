from airflow.models import Variable

DAG_ID = Variable.get("DPC_DAG_ID")
SCHEDULE_INTERVAL = Variable.get("DPC_SCHEDULE_INTERVAL")
