from datetime import datetime
from time import sleep
from random import randrange
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from resources.utils.date import TIMEZONE


def sleep_in(index: int) -> None:
    random_number = randrange(start=20 - index, stop=40 + index)

    print(f"Sleeping for {random_number} seconds.")
    sleep(random_number)


TABLES = {
    "ingestion.trusted.pokeapi": ["table_002", "table_003"],
    "ingestion.trusted.scoobydb": ["table_001", "table_005"],
}

with DAG(
    dag_id="modeling.refined",
    start_date=datetime(2023, 12, 2, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
    schedule="@daily",
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

    for dag, dag_tasks in TABLES.items():
        tasks[dag] = ExternalTaskSensor(
            task_id=dag,
            external_dag_id=dag,
            external_task_ids=dag_tasks,
            poke_interval=2,
            timeout=60*2,
            check_existence=True,
        )

        tasks["start"].set_downstream(tasks[dag])
        tasks[dag].set_downstream(tasks["modeling"])

    tasks["modeling"].set_downstream(tasks["stop"])
