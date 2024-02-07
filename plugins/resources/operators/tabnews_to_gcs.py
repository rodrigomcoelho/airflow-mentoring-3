
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from resources.hoooks.tabnews_hook import TabNewsHook

class TabNewsToGCSOperator(BaseOperator):
    def __init__(self, task_id: str, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.log.info("Criando operador TabNewsToGCS")
        self.__hook = TabNewsHook()

    def execute(self, context: Context) -> None:
        self.log.info("Estou dentro do nosso operador")
        self.__hook.fetch()
        self.log.info("Saindo do operador")
