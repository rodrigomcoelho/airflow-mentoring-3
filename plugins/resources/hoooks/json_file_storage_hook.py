from airflow.hooks.base import BaseHook

class JSONFileStorageHook(BaseHook):
    def __init__(self):
        super().__init__()

    def save_to_json(self, content: list[dict]) -> None:
        self.log.info("Estou no hook 'JSONFileStorageHook'.")
        self.log.warning(content[0])
