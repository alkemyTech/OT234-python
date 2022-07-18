""""
configuracion de DAG, sin consultas, 
ni procesamiento para la universidad Universidad J. F. Kennedy
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
import logging

#Se usaran en un futuro: 
# postgres operator para la extraccion de datos
# python operator para la transformacion
# python operator para la carga de datos

# Logging configuration
# creo el logger
logger = logging.getLogger('DAG_logger')
logger.setLevel(logging.DEBUG)

# formato del logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

# defino el console handler y seteo el level a DEBUG
#Level DEBUG se utiliza para informacion detallada, usualmente de nuestro interes
#para diagnosticar problemas
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

# defino el file handler y seteo el level a DEBUG
fh = logging.FileHandler('dag_OT234-34_UJFK.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)



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
    default_args=default_args,
    template_searchpath='/home/tomasreuque/Desktop/OT234-python/airflow/include',
    catchup=False

) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load