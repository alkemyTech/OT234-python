from airflow import DAG
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pathlib  import Path
import pandas as pd
import logging
import data_transformation_functions as tf

# Contstant variables
CONN_ID = 'alkemy_db'
TABLE = 'training'
S3_CONN_ID = 's3_alkemi'
BUCKET_NAME = 'cohorte-junio-a192d78b'
PARENT_PATH = Path(__file__).parent.absolute().parent
SQL_PATH = PARENT_PATH.joinpath('include')
FILES_PATH = PARENT_PATH.joinpath('files')
PROCESSED_DATA_PATH = PARENT_PATH.joinpath('datasets')

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
fh = logging.FileHandler(FILES_PATH.joinpath('DAG-UAIn_2020-09-01_2021-02-01_OT234-32-40-48.log'))
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
# add ch and fh to logger
logger.addHandler(ch)
logger.addHandler(fh)

# Functions called at the PythonOperators
def extract_from_db():
    '''
    This function connects to Postgres DB setted up as CONN_ID and runs the query from SQL_PATH over the TABLE.
    It saves data extracted in RAW_DATA_PATH as raw_data.csv.
    The values used are:
        CONN_ID = 'alkemy_db'
        TABLE = 'training'
        SQL_PATH = 'include/'
        RAW_DATA_PATH = 'datasets/'
    '''
    # Reads query in sql file
    with open(SQL_PATH.joinpath('UAIn_2020-09-01_2021-02-01_OT234-16.sql'), 'r') as file:
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
    pd.read_sql(sql, con=conn).to_csv(FILES_PATH.joinpath('UAIn_raw_data.csv'))
    logger.debug(f'Finished data extraction task.')

def transform_extrated_data():
    """
    This function transforms the extracted data.
    It saves data processed in PROCESSED_DATA_PATH as data.csv.
    The values used are:
        PROCESSED_DATA_PATH = 'files/dataset/'
    """
    raw_data = pd.read_csv(FILES_PATH.joinpath('UAIn_raw_data.csv'), index_col='Unnamed: 0')
    dataset = tf.transform_OT234_72(raw_data)
    try: 
        dataset.to_csv(PROCESSED_DATA_PATH.joinpath('UAIn_dataset.csv'))
        logger.debug(f'Dataset succesfully saved in {PROCESSED_DATA_PATH}.')
    except:
        logger.error('There was an error saving the dataset.')
        return

def load_to_s3():
    """
    This functions runs a S3Hook to upload the final dataset to S3.
    """
    dataset = pd.read_csv(PROCESSED_DATA_PATH.joinpath('UAIn_dataset.csv'), index_col='Unnamed: 0')
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_hook.load_string(dataset.to_csv(index=False),
                        '{0}.csv'.format('UAIn_dataset'),
                        bucket_name=BUCKET_NAME,
                        replace=True)
    try:
        assert s3_hook.get_key('UAIn_dataset.csv',BUCKET_NAME).key == 'UAIn_dataset.csv'
        logger.debug('The file was succesfully saved in s3.')
    except AssertionError:
        logger.error("The file couldn't be upload.")

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
            python_callable=transform_extrated_data
            )

        load_data = PythonOperator(
            task_id='load_data',
            python_callable=load_to_s3
            )
        
        extract_data >> transform_data >> load_data
        