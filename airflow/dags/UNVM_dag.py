from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging

## Realizar un log al empezar cada DAG con el nombre del logger
## Formato del log: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d')

def extract():
    logging.info('Extract process started.')

def trasnform():
    logging.info('Transform process started.')

def load():
    logging.info('Load process started.')
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'dag_UNVM',
    default_args=default_args,
    description='DAG ETL para Universidad Nacional De Villa María',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today(),
    catchup=False,
    tags=['example'],

) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load