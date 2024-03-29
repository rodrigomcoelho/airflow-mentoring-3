from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from resources.utils.datasets import SCOOBYDB_DATASET_TRUSTED
from resources.utils.date import TIMEZONE

TABLES = ["table_001", "table_002", "table_003", "table_004", "table_005"]


with DAG(
    dag_id="ingestion.trusted.scoobydb",
    schedule=None,
    start_date=datetime(2023, 11, 1, tzinfo=TIMEZONE),
    tags=["trusted", "scoobydb"],
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    for table in TABLES:
        tasks[table] = PythonOperator(
            task_id=table,
            python_callable=lambda: sleep(10),
            outlets=[SCOOBYDB_DATASET_TRUSTED],
        )

        tasks["start"].set_downstream(tasks[table])
        tasks[table].set_downstream(tasks["stop"])
