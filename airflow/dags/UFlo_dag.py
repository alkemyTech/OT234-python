
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator as PO

## Realizar un log al empezar cada DAG con el nombre del logger
## Formato del log: %Y-%m-%d - nombre_logger - mensaje
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d')

def extract():
    logging.info('Extract process started.')

def transform():
    logging.info('Transform process started.')

def load():
    logging.info('Load process started.')

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

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
    'dag_UFlo',
    default_args=default_args,
    description='DAG ETL para Universidad De Flores',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today(),
    catchup=False
) as dag:
    extract= PO(
        task_id='extract',
        postgres_conn_id="postgres_default",
        sql="../include/UFlo_2020-09-01_2021-02-01_OT234-12.sql"
        )
    
    transform= PythonOperator(task_id='transform',
    python_callable=transform)

    load= DummyOperator(task_id='load')

    extract >> transform >> load