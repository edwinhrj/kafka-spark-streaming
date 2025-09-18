import json
import requests
from kafka import KafkaProducer

# import airflow packages to automate the streaming of data from the api
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

# this default argument is applied to all tasks in the DAG
# means the earliest date it will run the DAG is 2025-09-18
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 9, 18),
}

def get_data():
    # data_json is the raw json object from the API
    data_json = requests.get("https://randomuser.me/api/")
    # returns only the user info from the whole dict object
    data_dict = data_json.json()
    return data_dict['results'][0] # only returns the user info

def format_data(data):
    data = {
        'first_name': data['name']['first'],
        'last_name': data['name']['last'],
        'email': data['email']
    }
    return data

def stream_data():
    data = get_data()
    formatted_data = format_data(data)

    # since this function is called by airflow, we need to connect to the kafka broker
    # running in the docker container -> cannot use localhost:9092
    # initializes the producer
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    curr_time = time.time()

    # continuously calls get_data and format_data for 60 seconds
    # efectively 'streams' data for 60 seconds
    while time.time() < curr_time + 60:
        try:
            data = get_data()
            formatted_data = format_data(data)
            # producer sends the formatted data to the kafka topic 'users_created'
            producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))
        except Exception as e:
            logging.error(f'an error occured{e}')

# defines the DAG, scheduling it to run daily
# and prevents it from running all past dates
with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False) as dag:

    # defines the airflow task
    # python operator allows us to run python functions inside airflow
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data # calls stream_data function as a python function
    )

