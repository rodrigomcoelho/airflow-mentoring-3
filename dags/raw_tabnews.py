from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from resources.operators.tabnews_to_minio import TabNewsToMinIO
from resources.utils.date import TIMEZONE

ENDPOINTS = ["contents", "status"]

with DAG(
    dag_id="ingestion.raw.tabnews",
    schedule="@daily",
    start_date=datetime(2024, 3, 20, tzinfo=TIMEZONE),
    catchup=True,
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
        tasks[endpoint] = TabNewsToMinIO(
            task_id=endpoint,
            conn_id="conn_tabnews",
            storage_conn="conn_storage",
            endpoint=endpoint,
            pool="tabnews",
            bucket_name="raw",
            root=["tabnews", endpoint],
        )

        tasks["start"].set_downstream(tasks[endpoint])
        tasks[endpoint].set_downstream(tasks["stop"])
