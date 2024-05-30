"""Kafka Stream
"""
import time
import logging
import json
import requests
import uuid
from datetime import datetime
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'tranhuy',
    'start_date': datetime(2024, 5, 26, 16, 00)
}

def get_data():
    """Lấy dữ liệu từ api

    Returns:
        res: Dữ liệu trả về từ api
    """
    res = requests.get("https://randomuser.me/api/", timeout=60)
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    """Cấu trúc lại dữ liệu

    Args:
        res (json): Dữ liệu gốc

    Returns:
        json: Dữ liệu sau chuyển đổi
    """
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    """Stream trong vòng 1 phút
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], \
                             max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60:
            break
        try:
            res = get_data()
            res = format_data(res=res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error('An error occured: %s', (e))
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
