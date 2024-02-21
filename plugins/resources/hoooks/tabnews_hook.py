from airflow.hooks.base import BaseHook

from requests import request, Response
import json

class TabNewsHook(BaseHook):
    def __init__(self):
        self.log.info("Criando Hook")

    def fetch(self) -> None:
        self.log.info("Fetching information from API")
        response: Response = request(method="GET", url="https://www.tabnews.com.br/api/v1/contents")
        response.raise_for_status()
        content = json.loads(response.content)

        return content
