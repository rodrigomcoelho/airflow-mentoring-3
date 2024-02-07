from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from resources.operators.tabnews_to_gcs import TabNewsToGCSOperator
from resources.utils.date import TIMEZONE

ENDPOINTS = ["contents"]

with DAG(
    dag_id="ingestion.raw.tabnews",
    schedule="@daily",
    start_date=datetime(2024, 2, 1, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
    tags=[
        "raw",
        "tabnews",
    ],
    max_active_runs=1,
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    for endpoint in ENDPOINTS:
        tasks[endpoint] = TabNewsToGCSOperator(task_id=endpoint)

        tasks["start"].set_downstream(tasks[endpoint])
        tasks[endpoint].set_downstream(tasks["stop"])