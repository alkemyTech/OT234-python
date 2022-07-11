# DAG for University processing - Group C
# Universidad Nacional de Jujuy
# Start airflow scheduler in OT234-python/airflow directory

from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import true
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
from Data_Processing_UNJu import Process_UNJu


############ Create logger
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
SQLquery = cwd + '/Include/UNJu_2020-09-01_2021-02-01_OT234-14.sql'
pathTOraw = cwd + '/files/'
pathTOdatasets = cwd +'/datasets/'
University='UNJu'

#############


def uploadS3(Univ, datasets_path):

    S3_CONN_ID = 'S3-OT234'
    BUCKET = 'cohorte-junio-a192d78b'
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_key = 'AKIAS2JWQJCDAOV62MGH'

    txt_path = datasets_path + Univ + '_dataset.txt'
    #File upload
    s3_hook.load_file(txt_path, key=s3_key, bucket_name=BUCKET, replace=true)


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
    task_id='Query.to.csv_UNJu',
    python_callable=queryTOcsv,
    op_kwargs={'Univ' : University, 'raw_path' : pathTOraw, 'sql_query' : SQLquery},
    dag=dag,
    )
    
    Process_Jujuy=PythonOperator(
    task_id='Data-Process_UNJu',
    python_callable=Process_UNJu,
    op_kwargs={'Univ' : University, 'raw_path' : pathTOraw, 'datasets_path' : pathTOdatasets},
    dag=dag,
    )

    Upload_Jujuy=PythonOperator(
    task_id='S3-Data-Upload_UNJu',
    python_callable=uploadS3,
    op_kwargs={'Univ' : University, 'datasets_path' : pathTOdatasets},
    dag=dag,
    )
    
    Query_Jujuy >> Process_Jujuy >> Upload_Jujuy
