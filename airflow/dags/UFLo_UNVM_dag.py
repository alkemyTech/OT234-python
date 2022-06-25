from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

"""
A futuro probablemente usaré:
     un custom operator (datasourcetocsv) --> Extracción desde el servidor y creación de csv
     un python operator que ejecute diferentes funciones de python --> Transformación de los datasets
     un python operator que ejecute funciones para amazon cloud --> Subida de archivos a amazon. 
"""

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
    UFLo_query = EmptyOperator(
        task_id = 'UFLo_query'
        )

    UFLo_transform = PythonOperator(
        task_id='UFLo_transform',
        python_callable=dummy
        )

    UFLo_amazon = EmptyOperator(
        task_id='UFLo_amazon',
        )

    UNVM_query = EmptyOperator(
        task_id = 'UNVM_query'
        )

    UNVM_transform = PythonOperator(
        task_id='UNVM_transform',
        python_callable=dummy
        )

    UNVM_amazon = EmptyOperator(
        task_id='UNVM_amazon',
        )

#UFLo_query >> UFLo_transform >> UFLo_amazon
#UNVM_query >> UNVM_transform >> UNVM_amazon

    
   
    
    