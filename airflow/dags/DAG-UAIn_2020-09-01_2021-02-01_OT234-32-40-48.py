from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import logging
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
fh = logging.FileHandler('airflow/files/DAG-UAIn_2020-09-01_2021-02-01_OT234-32-40-48.log')

fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
# add ch and fh to logger
logger.addHandler(ch)
logger.addHandler(fh)

# Contstant variables 
CONN_ID = 'alkemy_db'
TABLE = 'training'
SQL_PATH = 'airflow/include/'
RAW_DATA_PATH = 'airflow/files/'
PROCESSED_DATA_PATH = 'airflow/datasets/'

# Functions called at the PythonOperators
def extract_from_db():
    '''
    This function connects to Postgres DB setted up as CONN_ID and runs the query from SQL_PATH over the TABLE.
    It saves data extracted in RAW_DATA_PATH as raw_data.csv.
    The values used are:
        CONN_ID = 'alkemy_db'
        TABLE = 'training'
        SQL_PATH = 'airflow/include/'
        RAW_DATA_PATH = 'airflow/datasets/'
    '''
    # Reads query in sql file
    with open(SQL_PATH + 'UAIn_2020-09-01_2021-02-01_OT234-16.sql', 'r') as file:
        sql = file.read()
    # Connects to Postgres DB
    pg_hook = PostgresHook(
        postgres_conn_id=CONN_ID,
        schema=TABLE
    )
    try:
        conn = pg_hook.get_conn()
        logger.debug(f'Connection to the DB using {CONN_ID} was succesfully.')
    except:
        logger.error(f'{CONN_ID} did not work to connect to the DB.')
        return
    # Runs query and saves data
    pd.read_sql(sql, con=conn).to_csv(RAW_DATA_PATH + 'UAIn_raw_data.csv')
    logger.debug(f'Finished data extraction task.')

def transform_data_extrated():
    '''
    This function transforms the data extracted.
    It saves data processed in PROCESSED_DATA_PATH as data.csv.
    The values used are:
        PROCESSED_DATA_PATH = 'airflow/files/dataset/'
    '''
    raw_data = pd.read_csv(RAW_DATA_PATH + 'UAIn_raw_data.csv', index_col='Unnamed: 0')
    pass
    data = raw_data
    data.to_csv(PROCESSED_DATA_PATH + 'UAIn_dataset.csv')


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
        
        extract_data = PythonOperator(
            task_id='extract_data',
            python_callable=extract_from_db,
            )
        
        transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=transform_data_extrated
            )

        load_data = DummyOperator(
            # PythonOperator and S3Hook
            # We could create a custom function that takes transformed data in previuos task and load to S3 using the S3Hook. 
            # As we did at the extrac_data task we should set the connection at the Airflow UI in Admins/Connections and then called by id.
            task_id='load_data'
            )
        
        extract_data >> transform_data >> load_data
