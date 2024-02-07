from datetime import datetime
from random import randrange
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from resources.utils.datasets import (
    LINX_DATASET_TRUSTED,
    POKEAPI_DATASET_TRUSTED,
    SCOOBYDB_DATASET_TRUSTED,
)
from resources.utils.date import TIMEZONE


def sleep_in(index: int) -> None:
    random_number = randrange(start=20 - index, stop=40 + index)

    print(f"Sleeping for {random_number} seconds.")
    sleep(random_number)


TABLES = {
    "ingestion.trusted.pokeapi": {
        "tasks": ["table_002", "table_003"],
    },
    "ingestion.trusted.scoobydb": {
        "tasks": ["table_001", "table_005"],
    },
    "ingestion.trusted.spreadsheets": {
        "tasks": ["sheet_002", "sheet_003"],
        "timedelta": lambda x: datetime(
            year=x.year, month=x.month, day=1, hour=x.hour, tzinfo=x.tzinfo
        ),
    },
}

with DAG(
    dag_id="modeling.refined2",
    start_date=datetime(2023, 11, 1, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": False,
    },
    schedule=[POKEAPI_DATASET_TRUSTED, SCOOBYDB_DATASET_TRUSTED, LINX_DATASET_TRUSTED],
    tags=["refined"],
    catchup=True,
    max_active_runs=1,
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
        "modeling": PythonOperator(
            task_id="modeling",
            python_callable=lambda: sleep(10),
        ),
    }

    # for dag, options in TABLES.items():
    #    dag_tasks = options.get("tasks", [])
    # execution_fn = options.get("timedelta", lambda x: x)

    # tasks[dag] = ExternalTaskSensor(
    #     task_id=dag,
    #     external_dag_id=dag,
    #     external_task_ids=dag_tasks,
    #     poke_interval=2,
    #     execution_date_fn=execution_fn,
    #     timeout=60 * 2,
    #     check_existence=True,
    # )

    # tasks["start"].set_downstream(tasks[dag])
    tasks["start"].set_downstream(tasks["modeling"])

    tasks["modeling"].set_downstream(tasks["stop"])
