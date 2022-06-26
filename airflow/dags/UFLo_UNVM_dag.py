from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datasourcetocsv_operator import DataSourceToCsvOperator

from datetime import datetime

now = datetime.now()
current_time = now.strftime("%d/%m:%y")

dataset_dir = "/home/jvera/gitRepos/OT234-python/airflow/datasets/"
sql_dir = "/home/jvera/gitRepos/OT234-python/airflow/include/"

def sqlFileToQuery(path):
    query = ''
    with open(path) as file:
        for line in file:
            query += line
    return query

def dummy():
    return True

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

with DAG(
    'UNTF_2020-09-01_2021-02-01',
    description='dag para la univesidad UNTF',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,6,16),
    default_args=default_args,

) as dag:
    UFLo_query = DataSourceToCsvOperator(
        task_id='UFLO_query',
        sql= sqlFileToQuery(sql_dir + "UNTF_2020-09-01_2021-02-01_OT234-12.sql"),
        postgres_conn_id="postgres_server",
        database="training",
        output_dir= dataset_dir + 'UFLo_2020-09-01_2021-02-01.csv'
        #output_dir= dataset_dir + 'UFLo_2020-09-01_2021-02-01-{}.csv'.format(current_time)
        )

    UFLo_transform = PythonOperator(
        task_id='UFLo_transform',
        python_callable=dummy
        )

    UFLo_amazon = EmptyOperator(
        task_id='UFLo_amazon',
        )

    UNVM_query = DataSourceToCsvOperator(
        task_id='UNVM_query',
        sql=sqlFileToQuery(sql_dir + "UTNa_2020-09-01_2021-02-01_OT234-12.sql"),
        postgres_conn_id="postgres_server",
        database="training",
        output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01.csv'
        # output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01-{}.csv'.format(current_time)
        )

    UNVM_transform = PythonOperator(
        task_id='UNVM_transform',
        python_callable=dummy
        )

    UNVM_amazon = EmptyOperator(
        task_id='UNVM_amazon',
        )

UFLo_query >> UFLo_transform >> UFLo_amazon

UNVM_query >> UNVM_transform >> UNVM_amazon

    
   
    
    