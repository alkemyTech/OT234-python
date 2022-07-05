# DAG for University processing - Group C
# Universidad de Palermo / Universidad Nacional de Jujuy
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
from Data_Processing_UPal import Process_UPal


#############Create logger
logger = logging.getLogger('Universidad de Palermo')
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
SQLquery = cwd + '/Include/UPal_2020-09-01_2021-02-01_OT234-14.sql'
pathTOraw = cwd + '/files/'
pathTOdatasets = cwd +'/datasets/'
University='UPal'

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
    dag_id='dag_OT234_UPal',
    description='Dag procesamiento Universidad de Palermo',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,6,27),
    max_active_runs=3,
    catchup=False

)  as dag:

    Query_Palermo=PythonOperator(
    task_id='Query.to.csv_UPal',
    python_callable=queryTOcsv,
    op_kwargs={'Univ' : University, 'raw_path' : pathTOraw, 'sql_query' : SQLquery},
    dag=dag,
    )
    
    Process_Palermo=PythonOperator(
    task_id='Data-Process_UPal',
    python_callable=Process_UPal,
    op_kwargs={'Univ' : University, 'raw_path' : pathTOraw, 'datasets_path' : pathTOdatasets},
    dag=dag,
    )
    
    Upload_Palermo=PythonOperator(
    task_id='S3-Data-Upload_UPal',
    python_callable=uploadS3,
    op_kwargs={'Univ' : University, 'datasets_path' : pathTOdatasets},
    dag=dag,
    )

    Query_Palermo >> Process_Palermo >> Upload_Palermo