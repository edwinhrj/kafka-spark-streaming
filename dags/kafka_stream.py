import json
import requests
from kafka import KafkaProducer

# import airflow packages to automate the streaming of data from the api
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import logging

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

    # initializes a Kafka producer, which connects to Kafka broker running locally
    # on port 9092 (defined in the docker-compose.yml file)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    # producer sends the formatte data to the kafka topic 'users_created'
    producer.send('users_created', json.dumps(formatted_data).encode('utf-8'))

stream_data()