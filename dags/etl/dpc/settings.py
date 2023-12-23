from airflow.models import Variable

DAG_ID = Variable.get("DPCPCM_DAG_IG")
SCHEDULE_INTERVAL = Variable.get("DPCPCM_SCHEDULE_INTERVAL")
