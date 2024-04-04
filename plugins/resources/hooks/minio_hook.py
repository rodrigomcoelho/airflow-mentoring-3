import json
from datetime import datetime
from os.path import join

from airflow.providers.amazon.aws.operators.s3 import S3Hook

# {
#   "endpoint_url": "http://first-airflow-steps-minio-1:9000"
# }


class MinIOHook(S3Hook):
    def __init__(self, partition_date: str, *args, **kwargs) -> None:
        self.__partition_date = datetime.strptime(partition_date, "%Y-%m-%d")
        super().__init__(*args, **kwargs)

    def __get_file_path(self, root: list[str]) -> str:
        year = str(self.__partition_date.year).zfill(4)
        month = str(self.__partition_date.month).zfill(2)
        day = str(self.__partition_date.day).zfill(2)

        file_path = join(*root, f"year={year}", f"month={month}", f"day={day}")

        return file_path

    def save_to_storage(
        self, content: list[dict], bucket_name: str, root: list[str], file_name: str
    ) -> None:
        content_str: list[str] = [
            json.dumps(record, ensure_ascii=False) for record in content
        ]

        file = f"{file_name}.json"
        file_path = join(self.__get_file_path(root), file)

        self.load_string(
            string_data="\n".join(content_str),
            key=file_path,
            bucket_name=bucket_name,
        )

        self.log.info(f"Saved {file_path}")
