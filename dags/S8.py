TOKEN_BOT = "7143118018:AAFyyy_-RGzTOZNZr-L-qKI1EyNHA3mV4zU"
TOKEN_YANDEX = "bb3c563c-165c-4a2c-8b25-5a2adf9c514a"
TOKEN_OWM = "6ea20a03b4f3369fbe408ee23d8f86fa"

import requests
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.telegtam import TelegramOperator



def _detect_wether(ti):
    data = ti.xcom_pull(task_ids=['get_wether_openwether', 'get_wether_yandex'])
    return (data)
    # response = json.loads(data[0])
    # temperture = float(response["main"]["temp"]) - 273
    # response = requests.get("https://api.openweathermap.org/data/2.5/weather?q=Moscow&appid=6ea20a03b4f3369fbe408ee23d8f86fa&lang=ru")
    # rsp = json.loads(response.content.decode('utf-8'))
    # temperture = float(rsp["main"]["temp"]) - 273


def _telebot_send(ti):
    return str(ti[0]) + str(ti[1])


with DAG("wether_telegram", default_args={"retries": 1}, start_date=datetime(2021, 1 ,1), schedule='@daily', catchup=False) as dag:



    get_wether_openwether = SimpleHttpOperator(
        task_id='get_wether_openwether',
        method='GET',
        http_conn_id='http_wether',
        endpoint='/data/2.5/weather?q=Moscow&appid=6ea20a03b4f3369fbe408ee23d8f86fa&lang=ru',
        response_filter = lambda response : json.loads(response["main"]["temp"]),
        # headers={'Content-Type': 'application/json'},
        # xcom_push=True
        dag=dag
    )

    get_wether_yandex = SimpleHttpOperator(
        task_id='get_wether_yandex',
        method='GET',
        http_conn_id='http_yandex',
        endpoint='/v2/forecast?lat=52.37125&lon=4.89388',
        response_filter = lambda response : json.loads(response["temp"]),
        headers={'X-Yandex-Weather-Key': 'bb3c563c-165c-4a2c-8b25-5a2adf9c514a'},
        # xcom_push=True
        dag=dag
    )

    branch_wether = BranchPythonOperator(
        task_id="branch_wether",
        python_callable=_detect_wether
    )

    send_to_telbot = TelegramOperator(
        task_id="send_to_telbot",
        token=TOKEN_BOT,
        chat_id="7143118018",
        text=_telebot_send
    )


[get_wether_openwether, get_wether_yandex]  >> branch_wether >> send_to_telbot
