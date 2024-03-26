from jinja2 import Environment
from datetime import datetime


def get_dag_content() -> str:
    with open("./dags/raw_tabnews.py", encoding="utf-8") as file:
        return file.read()

dag_content = get_dag_content()

env = Environment()

render = env.from_string(dag_content)

new_dag = render.render(data_interval_start=datetime.now(), ds= True)

print(new_dag)
