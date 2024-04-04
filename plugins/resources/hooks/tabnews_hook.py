# https://www.tabnews.com.br/api/v1
import json
from time import sleep

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from requests import HTTPError, Response, request


class TabNewsHook(BaseHook):
    def __init__(self, conn_id: str):
        self.__conn: Connection = self.get_connection(conn_id)

    def fetch(self, endpoint: str) -> dict:
        self.log.info(f"Fetching endpoint {endpoint} from API.")
        retries = 3
        while True:
            try:
                response: Response = request(
                    method="GET",
                    url=f"{self.__conn.host}/{endpoint}",
                )
                # adicionando esse tempo para não ser bloqueado por API exhaustion.
                sleep(10)
                response.raise_for_status()
            except HTTPError as error:
                sleeping_time = (4 - retries) * 5  # 60
                sleep(sleeping_time)
                self.log.error(error)
                self.log.warning(f"New attempt will happen in {sleeping_time} minutes.")
                retries -= 1

                if retries > 0:
                    continue
            else:
                return json.loads(response.content)

        raise HTTPError("Unable to reach API")
