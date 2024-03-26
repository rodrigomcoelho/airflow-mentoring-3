import json
from os.path import join, exists
from os import mkdir
from airflow.hooks.base import BaseHook
from datetime import datetime
class JSONFileStorageHook(BaseHook):
    PATH = "/opt/airflow/data"

    def __init__(self, partition_date: str):
        self.__partition_date = datetime.strptime(partition_date, "%Y-%m-%d")

    def __get_file_path(self) -> str:
        year = str(self.__partition_date.year).zfill(4)
        month= str(self.__partition_date.month).zfill(2)
        day = str(self.__partition_date.day).zfill(2)

        file_path = JSONFileStorageHook.PATH
        for dir_path in [f"year={year}", f"month={month}", f"day={day}"]:
            file_path = join(file_path, dir_path)
            if exists(file_path):
                continue
            mkdir(file_path)

        return file_path

    def save_to_json(self, content: list[dict], file_name: str) -> None:
        file = f"{file_name}.json"

        file_path = join(self.__get_file_path(), file)
        with open(file_path, mode="w", encoding="utf-8") as file:
            file.write(json.dumps(content, indent=2, ensure_ascii=False))
            file.write("\n")

        self.log.info(f"Arquivo '{file_path}' salvo com sucesso.")
