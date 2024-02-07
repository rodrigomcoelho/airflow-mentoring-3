"""
Minha **DAG** rodando
"""
from datetime import datetime
from random import randrange
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from resources.utils.date import TIMEZONE


def sleep_in(index: int) -> None:
    random_number = randrange(start=20 - index, stop=40 + index)

    print(f"Sleeping for {random_number} seconds.")
    sleep(random_number)


TABLES = ["table_001", "table_002", "table_003", "table_004", "table_005"]


with DAG(
    dag_id="ingestion.raw.pokeapi",
    start_date=datetime(2023, 11, 1, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
    schedule="@daily",
    tags=["raw", "pokeapi"],
    catchup=True,
    description="Minha DAG de Ingestão",
    doc_md=__doc__,
    max_active_runs=1,
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "trigger": TriggerDagRunOperator(
            task_id="trigger",
            trigger_dag_id="ingestion.trusted.pokeapi",
            execution_date="{{ data_interval_start }}",
        ),
        "stop": EmptyOperator(task_id="stop"),
    }

    for index, table in enumerate(TABLES):
        tasks[table] = PythonOperator(
            task_id=table, python_callable=sleep_in, op_kwargs={"index": index}
        )

        tasks["start"].set_downstream(tasks[table])  # start >> stop
        tasks[table].set_downstream(tasks["trigger"])  # start >> stop
        tasks["stop"].set_upstream(tasks["trigger"])
