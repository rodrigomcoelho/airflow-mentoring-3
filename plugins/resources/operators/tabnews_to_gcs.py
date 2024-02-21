from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from resources.hoooks.tabnews_hook import TabNewsHook
from resources.hoooks.json_file_storage_hook import JSONFileStorageHook

class TabNewsToJSONFileOperator(BaseOperator):
    def __init__(self, task_id: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.log.info("Criando operador TabNewsToGCS")
        self.__hook = TabNewsHook()
        self.__storage_hook = JSONFileStorageHook()

    def execute(self, context: Context) -> None:
        self.log.info("Estou dentro do nosso operador")
        content = self.__hook.fetch()
        self.__storage_hook.save_to_json(content=content)
        self.log.info("Saindo do operador")
