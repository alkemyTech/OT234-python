# DAG sin consultas ni procesamiento para el grupo de universidades C
# Universidad de Palermo

from airflow import DAG
from datetime import timedelta, datetime
import pandas as pd
import psycopg2
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging


##### Create logger
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

# 'application' code
#logger.debug('debug message')
logger.info('Mensaje base de datos')
#logger.warning('warn message')
#logger.error('error message')
#logger.critical('critical message')

#######


#Conección Base de datos (incompleta)
HOST_id='training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com'


###Configuración Retries
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}
######


with DAG('dag_OT234-30_UPal',
    description='Dag procesamiento Universidad de Palermo',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,6,20),
    template_searchpath='/OT234-python/airflow/include',

) as dag:
    
    Query_Palermo = DummyOperator(
        task_id='Query_Palermo',
    #    postgres_conn_id=HOST_id,
    #    sql='UPal_2020-09-01_2021-02-01_OT234-14.sql'
        )

    Proc_Pandas = DummyOperator(
        task_id='Proc_Pandas',
        )

    Upload_S3 = DummyOperator(
        task_id='Upload_S3',
        )

    Query_Palermo >> Proc_Pandas >> Upload_S3