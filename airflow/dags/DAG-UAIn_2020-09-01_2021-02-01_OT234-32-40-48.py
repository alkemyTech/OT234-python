from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
import logging

# Imports that we would make if we should work with real tasks

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.python import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook
# import pandas as pd

# Logging configuration
# create logger
logger = logging.getLogger('DAG_logger')
logger.setLevel(logging.DEBUG)
# format logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
fh = logging.FileHandler('airflow/DAG-UAIn_2020-09-01_2021-02-01_OT234-32-40-48.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
# add ch and fh to logger
logger.addHandler(ch)
logger.addHandler(fh)

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    # DAG Universidad Abierta Interamericana
    dag_id='DAG-UAIn_2020-09-01_2021-02-01_OT234-32-40-48',
    start_date=datetime(2022, 6, 18),
    max_active_runs=3,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
    ) as dag:
        
        extract_data = DummyOperator(
            # extract_data
            # We could do this task by two differents ways:

            # 1) PostgresOperator
            # Using PostgresOperator to connect to postgres DB and run the file.sql found in the INCLUDE directory.
            # The connection used is setted at the Airflow UI in Admins/Connections and then called by id. 
            # If we use this operator we have to push the data to the next task setting True next parameter:
            # xcom_push=True
        
            # 2) PythonOperator with PostgresHook'
            # Using the PythonOperator to run a custom function that uses PostgresHook to connect to postgres DB and run 
            # the DAG-UAIn_2020-09-01_2021-02-01_OT234-16.sql found inthe INCLUDE directory.
            # with open('/.../OT234-python/airflow/include/DAG-UAIn_2020-09-01_2021-02-01_OT234-16.sql', 'r') as file:
            #        query = file.read()
            # The connection used is setted at the Airflow UI in Admins/Connections and then called by id.
            # When the connection is made we could execute the query with pd.read_sql function to return a data frame with the results.
            task_id='extract_data'
            )
        
        transform_data = DummyOperator(
            # PythonOperator
            # We could process data with pandas to transform them.
            task_id='transform_data'
            )

        load_data = DummyOperator(
            # PythonOperator and S3Hook
            # We could create a custom function that takes transformed data in previuos task and load to S3 using the S3Hook. 
            # As we did at the extrac_data task we should set the connection at the Airflow UI in Admins/Connections and then called by id.
            task_id='load_data'
            )
        
        extract_data >> transform_data >> load_data
