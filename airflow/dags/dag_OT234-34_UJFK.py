""""
configuracion de DAG, sin consultas, 
ni procesamiento para la universidad Universidad J. F. Kennedy
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator

<<<<<<< HEAD

=======
>>>>>>> main
#Se usaran en un futuro: 
# postgres operator para la extraccion de datos
# python operator para la transformacion
# python operator para la carga de datos

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'dag_OT234-34_UJFK',
    description='DAG ETL para Universidad J. F. Kennedy',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today(),
    default_args=default_args
) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load