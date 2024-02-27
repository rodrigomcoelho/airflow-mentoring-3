from airflow.hooks.base import BaseHook
import json
class JSONFileStorageHook(BaseHook):
    def __init__(self):
        super().__init__()

    def save_to_json(self, content: list[dict], file_name: str) -> None:
        self.log.info("Estou no hook 'JSONFileStorageHook'.")
        path = "/opt/airflow/data"
        file = f"{file_name}.json"

        file_path = f"{path}/{file}"
        with open(file_path, mode="w", encoding="utf-8") as file:
            file.write(json.dumps(content, indent=2, ensure_ascii=False))
            file.write("\n")

        self.log.info(f"Arquivo '{file_path}' salvo com sucesso.")
