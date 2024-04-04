from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from resources.hooks.minio_hook import MinIOHook
from resources.hooks.tabnews_hook import TabNewsHook


class TabNewsToMinIO(BaseOperator):

    template_fields = "_partition_date"

    def __init__(
        self,
        task_id: str,
        conn_id: str,
        endpoint: str,
        bucket_name: str,
        root: list[str] = [],
        partition_date: str = "{{ ds }}",
        storage_conn: str = "conn_storage",
        **kwargs,
    ):
        self.__endpoint = endpoint
        self.__hook = TabNewsHook(conn_id=conn_id)
        self._partition_date = partition_date
        self.__storage_conn = storage_conn
        self.__bucket_name = bucket_name
        self.__root = root
        self.__storage_hook: MinIOHook = (
            None  #  JSONFileStorageHook(partition_date=self._partition_date)
        )
        super().__init__(task_id=task_id, **kwargs)

    @property
    def storage_hook(self) -> MinIOHook:
        if self.__storage_hook:
            return self.__storage_hook

        self.__storage_hook = MinIOHook(
            aws_conn_id=self.__storage_conn,
            partition_date=self._partition_date,
        )

        return self.__storage_hook

    def execute(self, context: Context) -> None:
        content = self.__hook.fetch(endpoint=self.__endpoint)
        self.storage_hook.save_to_storage(
            content=content,
            bucket_name=self.__bucket_name,
            root=self.__root,
            file_name=self.__endpoint,
        )
