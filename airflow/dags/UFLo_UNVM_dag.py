from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

"""
A futuro probablemente usaré:
     un custom operator (datasourcetocsv) --> Extracción desde el servidor y creación de csv
     un python operator que ejecute diferentes funciones de python --> Transformación de los datasets
     un python operator que ejecute funciones para amazon cloud --> Subida de archivos a amazon. 
"""

with DAG(
    'UNTF_2020-09-01_2021-02-01',
    description='dag para la univesidad UNTF',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2022,6,16)
) as dag:
    t1 = EmptyOperator(task_id)(
        task_id='task_1',
        )

t1

    
   
    
    