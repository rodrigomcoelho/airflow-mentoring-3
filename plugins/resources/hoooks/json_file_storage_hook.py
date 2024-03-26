import json

from airflow.hooks.base import BaseHook

from datetime import datetime
from pendulum import timezone
class JSONFileStorageHook(BaseHook):
    def __init__(self):
        super().__init__()

    def save_to_json(self, content: list[dict], file_name: str) -> None:
        path = "/opt/airflow/data"
        file = f"{file_name}.json"

        # TODO: remover referencia do dia correte.
        now = datetime.now(tz=timezone("America/Sao_Paulo"))
        year = str(now.year).zfill(4)
        month= str(now.month).zfill(2)
        day = str(now.day).zfill(2)

        file_path = f"{path}/year={year}_month={month}_day={day}_{file}"
        with open(file_path, mode="w", encoding="utf-8") as file:
            file.write(json.dumps(content, indent=2, ensure_ascii=False))
            file.write("\n")

        self.log.info(f"Arquivo '{file_path}' salvo com sucesso.")
