# DAG sin consultas ni procesamiento para el grupo de universidades C
# Universidad Nacional de Jujuy

from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
import logging
import csv
import os
import sys


# Work directory and path to files
cwd = os.getcwd()
# Import modules from paths
sys.path.append(cwd + '/plugins/')
from query_to_csv import queryTOcsv



############# Create logger
logger = logging.getLogger('Universidad Nacional de Jujuy')
logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)

logger.info('Inicio')

############# Arguments Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}
#############

############# PARAMETERS 
sql_query = cwd + '/Include/UNJu_2020-09-01_2021-02-01_OT234-14.sql'
raw_path = cwd + '/files/'
Univ='UNJu'

#############

with DAG(
    dag_id='dag_OT234_UNJu',
    description='Dag procesamiento Universidad Nacional de Jujuy',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,6,27),
    max_active_runs=3,
    catchup=False

)  as dag:
    Query_Jujuy=PythonOperator(
    task_id='Query_Universidad-Nacional-de-Jujuy',
    python_callable=queryTOcsv,
    op_args={Univ, raw_path, sql_query},
    dag=dag,
    )
    
    Query_Jujuy
   
