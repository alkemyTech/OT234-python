from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import logging
import pandas as pd

CONN_ID = 'alkemy_db'
TABLE = 'training'
SQL_PATH = 'include/'
RAW_DATA_PATH = 'files/'
PROCESSED_DATA_PATH = 'datasets/'


# creo el logger
logger = logging.getLogger('DAG_logger')
logger.setLevel(logging.DEBUG)
# formato del logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')
# defino el console handler y seteo el level a DEBUG
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
# defino el file handler y seteo el level a DEBUG
fh = logging.FileHandler(RAW_DATA_PATH + 'dag_OT234-34_FLCS.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(fh)


# Functions called at the PythonOperators
def extract_from_db():
    # lee la consulta del archivo sql
    with open(SQL_PATH + 'FLCS_2020-09-01_2021-02-01_OT234-18.sql', 'r') as file:
        sql = file.read()
    # Conexion Postgres DB
    pg_hook = PostgresHook(
        postgres_conn_id=CONN_ID,
        schema=TABLE
    )
    try:
        conn = pg_hook.get_conn()
        logger.debug(f'Conexion a la BD {CONN_ID} se realizo con éxito.')
    except:
        logger.error(f'{CONN_ID} no se pudo conectar a la BD')
        return
    # Corre el query y guarda la data
    pd.read_sql(sql, con=conn).to_csv(PROCESSED_DATA_PATH + 'FLCS_raw_data.csv')
    logger.debug(f'Finalizo la extraccion')


default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    #DAG ETL para Facultad Latinoamericana De Ciencias Sociales
    dag_id='dag_OT234-34_FLCS',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today(),
    default_args=default_args,
    template_searchpath='/home/tomasreuque/Desktop/OT234-python/airflow/include',
    catchup=False

    ) as dag:

        extraer= PythonOperator(
            task_id='extraer',
            python_callable=extract_from_db,
            )

        transformar= EmptyOperator(task_id='transformar')

        cargar= EmptyOperator(task_id='cargar')

        extraer >> transformar >> cargar