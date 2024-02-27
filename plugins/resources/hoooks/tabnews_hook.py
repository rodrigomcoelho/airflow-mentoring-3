from airflow.hooks.base import BaseHook

from requests import request, Response
import json
from time import sleep
class TabNewsHook(BaseHook):
    def __init__(self):
        self.log.info("Criando Hook")

    def fetch(self, endpoint: str) -> dict:
        self.log.info("Fetching information from API")
        response: Response = request(method="GET", url=f"https://www.tabnews.com.br/api/v1/{endpoint}")
        response.raise_for_status()

        sleep(10) # adicionando esse tempo para não ser bloqueado por API exhaustion.
        content = json.loads(response.content)

        return content
