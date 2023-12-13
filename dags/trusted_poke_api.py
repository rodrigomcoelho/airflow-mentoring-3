from time import sleep

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from resources.utils.date import days_ago


TABLES = [
  "table_001",
  "table_002",
  "table_003",
  "table_004",
  "table_005"
]


with DAG(
    dag_id="ingestion.trusted.pokeapi",
    schedule=None,
    start_date=days_ago(n=10),
    tags=["trusted"],
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

        #sensor_task = f"sensor_{table}"

        # tasks[sensor_task] = ExternalTaskSensor(
        #     task_id=sensor_task,
        #     external_dag_id="ingestion.raw.pokeapi",
        #     external_task_id=table,
        #     timeout=60,
        #     poke_interval=5,
        #     check_existence=True,
        # )

        tasks[table] = PythonOperator(
            task_id=table,
            python_callable=lambda: sleep(10),
        )

        tasks["start"].set_downstream(tasks[table])
        #tasks[sensor_task].set_downstream(tasks[table_task])
        tasks[table].set_downstream(tasks["stop"])
