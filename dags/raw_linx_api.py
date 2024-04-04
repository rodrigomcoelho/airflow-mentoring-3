"""
Minha **DAG** rodando
"""

from datetime import datetime
from random import randrange
from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from resources.utils.datasets import LINX_DATASET_RAW
from resources.utils.date import TIMEZONE


def sleep_in(index: int) -> None:
    random_number = randrange(start=20 - index, stop=40 + index)
    sleep_phrase = f"Sleeping for {random_number} seconds."

    # with open(LINX_DATASET_RAW.uri, mode="a", encoding="utf-8") as file:
    #     file.write(f"{datetime.now().isoformat()} - {sleep_phrase}\n")

    print(sleep_phrase)
    sleep(random_number)


TABLES = ["table_001"]


with DAG(
    dag_id="ingestion.raw.linx",
    start_date=datetime(2023, 11, 1, tzinfo=TIMEZONE),
    default_args={
        "owner": "rodrigo",
        "depends_on_past": True,
    },
    schedule="@daily",
    tags=["raw", "linx"],
    catchup=True,
    description="Minha DAG de Ingestão",
    doc_md=__doc__,
    max_active_runs=1,
) as dag:
    tasks = {
        "start": EmptyOperator(task_id="start"),
        "stop": EmptyOperator(task_id="stop"),
    }

    for index, table in enumerate(TABLES):
        tasks[table] = PythonOperator(
            task_id=table,
            python_callable=sleep_in,
            op_kwargs={"index": index},
            outlets=[LINX_DATASET_RAW],
        )

        tasks["start"].set_downstream(tasks[table])
        tasks[table].set_downstream(tasks["stop"])
