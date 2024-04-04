from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from resources.hooks.json_file_storage_hook import JSONFileStorageHook
from resources.hooks.tabnews_hook import TabNewsHook


class TabNewsToJSONFileOperator(BaseOperator):

    template_fields = "_partition_date"

    def __init__(
        self,
        task_id: str,
        conn_id: str,
        endpoint: str,
        partition_date: str = "{{ ds }}",
        **kwargs,
    ):
        self.__endpoint = endpoint
        self.__hook = TabNewsHook(conn_id=conn_id)
        self._partition_date = partition_date
        self.__storage_hook: JSONFileStorageHook = (
            None  #  JSONFileStorageHook(partition_date=self._partition_date)
        )
        super().__init__(task_id=task_id, **kwargs)

    @property
    def storage_hook(self) -> JSONFileStorageHook:
        if self.__storage_hook:
            return self.__storage_hook

        self.__storage_hook = JSONFileStorageHook(partition_date=self._partition_date)

        return self.__storage_hook

    def execute(self, context: Context) -> None:
        content = self.__hook.fetch(endpoint=self.__endpoint)
        self.storage_hook.save_to_json(content=content, file_name=self.__endpoint)
