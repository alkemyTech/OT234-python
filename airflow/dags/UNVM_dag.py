

"""
Descripción
COMO: Analista de datos
QUIERO: Configurar un DAG, sin operators.
PARA: Hacer un ETL para 2 universidades distintas.

Criterios de aceptación: 
Configurar el DAG para las siguientes universidades:
Universidad De Flores
Universidad Nacional De Villa María

Documentar los operators que se deberían utilizar a futuro, teniendo en cuenta que se va a hacer dos consultas SQL (una para cada universidad), se van a procesar los datos con pandas y se van a cargar los datos en S3.  El DAG se debe ejecutar cada 1 hora, todos los días.
"""
"""
Nota: A futuro se utilizarán los siguientes modulos
* para conectar con la BBDD Postgres: airflow.providers.postgres.operators.postgres
* para procesar datos: pandas
* para cargar los datos a S3: --pendiente investigar
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging
from airflow.providers.postgres.operators.postgres import PostgresOperator as PO

# Realizar un log al empezar cada DAG con el nombre del logger
# Formato del log: %Y-%m-%d - nombre_logger - mensaje
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
    extract = PO(
        task_id='extract',
        postgres_conn_id="postgres_default",
        sql="../include/UNVM_2020-09-01_2021-02-01_OT234-12.sql"
    )
    transform = DummyOperator(task_id='transform')
    load = DummyOperator(task_id='load')

    extract >> transform >> load
    
