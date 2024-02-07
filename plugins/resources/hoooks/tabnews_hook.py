from airflow.hooks.base import BaseHook

class TabNewsHook(BaseHook):
    def __init__(self):
        self.log.info("Criando Hook")

    def fetch(self) -> None:
        self.log.info("Fetching information from API")
