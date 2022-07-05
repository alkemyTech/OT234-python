""""
configuracion de DAG, sin consultas, 
ni procesamiento para la universidad 
Facultad Latinoamericana De Ciencias Sociales
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

#Se usaran en un futuro: 
# postgres operator para la extraccion de datos
# python operator para la transformacion
# python operator para la carga de datos                  

with DAG(
    'dag_OT234-34_FLCS',
    description='DAG ETL para Facultad Latinoamericana De Ciencias Sociales',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today()
) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load