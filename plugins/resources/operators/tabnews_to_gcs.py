from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from resources.hoooks.json_file_storage_hook import JSONFileStorageHook
from resources.hoooks.tabnews_hook import TabNewsHook


class TabNewsToJSONFileOperator(BaseOperator):
    def __init__(self, task_id: str, endpoint: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.log.info("Criando operador TabNewsToGCS")
        self.__endpoint = endpoint
        self.__hook = TabNewsHook()
        self.__storage_hook = JSONFileStorageHook()

    def execute(self, context: Context) -> None:
        self.log.info("Estou dentro do nosso operador")
        content = self.__hook.fetch(endpoint=self.__endpoint)
        self.__storage_hook.save_to_json(content=content, file_name=self.__endpoint)
        self.log.info("Saindo do operador")
