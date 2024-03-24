'''
— Зарегистрируйтесь в ОрепWeatherApi (https://openweathermap.org/api)
— Создайте ETL, который получает температуру в заданной вами локации, и
дальше делает ветвление:

• В случае, если температура больше 15 градусов цельсия — идёт на ветку, в которой есть оператор, выводящий на
экран «тепло»;
• В случае, если температура ниже 15 градусов, идёт на ветку с оператором, который выводит в консоль «холодно».

Оператор ветвления должен выводить в консоль полученную от АРI температуру.

— Приложите скриншот графа и логов работы оператора ветвленния.
'''
import requests
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator



def _detect_wether(ti):
    data = ti.xcom_pull(task_ids=['get_http_data'])
    temperture = float(data["main"]["temp"]) - 273
    # response = requests.get("https://api.openweathermap.org/data/2.5/weather?q=Moscow&appid=6ea20a03b4f3369fbe408ee23d8f86fa&lang=ru")
    # rsp = json.loads(response.content.decode('utf-8'))
    # temperture = float(rsp["main"]["temp"]) - 273
    # print(temperture)
    # # temperture = 16
    if temperture >= 15:
       return "warm"
    else:
        return "cold"


with DAG("wether_detector", default_args={"retries": 1}, start_date=datetime(2021, 1 ,1), schedule='@daily', catchup=False) as dag:



    get_http_data = SimpleHttpOperator(
        task_id='get_http_data',
        method='GET',
        http_conn_id='http_wether',
        endpoint='/data/2.5/weather?q=Moscow&appid=6ea20a03b4f3369fbe408ee23d8f86fa&lang=ru',
        response_filter = lambda response : json.loads(response.content.decode('utf-8')),
        # headers={'Content-Type': 'application/json'},
        # xcom_push=True
        dag=dag
    )

    detect_wether = BranchPythonOperator(
        task_id="detect_wether",
        python_callable=_detect_wether
        )
    warm = BashOperator(
        task_id="warm",
        bash_command="echo 'warm'"
        )
    cold = BashOperator(
        task_id="cold",
        bash_command="echo 'cold'"
        )

get_http_data >> detect_wether >> [warm, cold]
