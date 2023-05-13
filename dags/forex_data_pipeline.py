from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


import csv
import requests
import json

# are applied to tasks 
default_args={
    "owner":"airflow",
    "email_on_failure" : False,
    "email_on_retry" : False,
    "email": "franklinmutwiri41@gmail.com",
    "retries":5,
    "retry_delay": timedelta(minutes=1)
}



def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open( '/opt/airflow/dags/files/forex_currencies.csv' ) as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        
        for idx, row in enumerate(reader):
            # print(row)
            base = row['base']
            with_pairs = row['with_pairs'].split(" ")
            response = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            # print(f"{BASE_URL}{ENDPOINTS[base]}")
            outdata = {'base': base, 'rates': {}, 'last_update': response['date'] }
            # print(outdata)
            for pair in with_pairs:
                # print(response['rates'][pair])
                outdata['rates'][pair] = response['rates'][pair]
                # print(outdata)
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')



with DAG (
   dag_id= "forex_data_pipeline",
   start_date= datetime(2023,5,11,1),
   schedule_interval= "@daily",
   default_args=default_args,
#    catchup=False
) as dag:
    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval = 5,
        timeout = 30
    )
    is_forex_raw_file_available = FileSensor(
        task_id = "is_forex_raw_file_available",
        fs_conn_id="forex_path",
        filepath= 'forex_currencies.csv',
        poke_interval = 5,
        timeout = 30
    )
    
    download_rates = PythonOperator(
        task_id = "download_rates",
        python_callable=download_rates
    )

    saving_rates_to_hadoop = BashOperator(
        task_id ="saving_rates_to_hadoop",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
            """
    )

    creating_forex_rates_table = HiveOperator(
    task_id="creating_forex_rates_table",
    hive_cli_conn_id="hive_conn",
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
            base STRING,
            last_update DATE,
            eur DOUBLE,
            usd DOUBLE,
            nzd DOUBLE,
            gbp DOUBLE,
            jpy DOUBLE,
            cad DOUBLE
            )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """
    )

    forex_processing = SparkSubmitOperator(
        task_id = "forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id ='spark_conn',
        verbose=False
    )




    is_forex_rates_available >> is_forex_raw_file_available >> download_rates>>saving_rates_to_hadoop >> creating_forex_rates_table

