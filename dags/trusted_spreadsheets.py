from datetime import datetime
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from resources.utils.date import TIMEZONE

SHEETS = ["sheet_001", "sheet_002", "sheet_003", "sheet_004", "sheet_005"]


with DAG(
    dag_id="ingestion.trusted.spreadsheets",
    schedule=None,
    start_date=datetime(2023, 10, 1, tzinfo=TIMEZONE),
    tags=["trusted", "spreadsheets"],
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    for sheet in SHEETS:
        sensor_task = f"sensor_{sheet}"

        tasks[sheet] = PythonOperator(
            task_id=sheet,
            python_callable=lambda: sleep(10),
        )

        tasks["start"].set_downstream(tasks[sheet])
        tasks[sheet].set_downstream(tasks["stop"])
