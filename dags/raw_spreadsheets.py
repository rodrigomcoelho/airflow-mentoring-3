"""
Minha **DAG** rodando
"""
from datetime import datetime
from time import sleep
from random import randrange
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from resources.utils.date import TIMEZONE


def sleep_in(index: int) -> None:
    random_number = randrange(start=5 - index, stop=10 + index)

    print(f"Sleeping for {random_number} seconds.")
    sleep(random_number)


SHEETS = ["sheet_001", "sheet_002", "sheet_003", "sheet_004", "sheet_005"]


with DAG(
    dag_id="ingestion.raw.spreadsheets",
    start_date=datetime(2023, 10, 1, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
    schedule="@monthly",
    tags=["raw", "spreadsheets"],
    catchup=True,
    description="Minha DAG de Ingestão",
    doc_md=__doc__,
    max_active_runs=1,
) as dag:
    tasks = {
       "start": EmptyOperator(task_id="start"),
        "trigger": TriggerDagRunOperator(
            task_id="trigger",
            trigger_dag_id="ingestion.trusted.spreadsheets",
            execution_date="{{ data_interval_start }}",
        ),
        "stop": EmptyOperator(task_id="stop"),
    }

    for index, sheet in enumerate(SHEETS):
        tasks[sheet] = PythonOperator(
            task_id=sheet, python_callable=sleep_in, op_kwargs={"index": index}
        )

        tasks["start"].set_downstream(tasks[sheet])  # start >> stop
        tasks[sheet].set_downstream(tasks["trigger"])  # start >> stop
        tasks["stop"].set_upstream(tasks["trigger"])
