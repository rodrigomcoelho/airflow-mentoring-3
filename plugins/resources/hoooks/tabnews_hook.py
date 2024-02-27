# https://www.tabnews.com.br/api/v1
import json
from time import sleep

from airflow.hooks.base import BaseHook
from requests import Response, request
from airflow.models.connection import Connection

class TabNewsHook(BaseHook):
    def __init__(self, conn_id: str):
        self.__conn: Connection = self.get_connection(conn_id)

    def fetch(self, endpoint: str) -> dict:
        self.log.info(f"Fetching endpoint {endpoint} from API.")

        response: Response = request(
            method="GET", url=f"{self.__conn.host}/{endpoint}"
        )
        response.raise_for_status()

        sleep(10)  # adicionando esse tempo para não ser bloqueado por API exhaustion.
        content = json.loads(response.content)

        return content
