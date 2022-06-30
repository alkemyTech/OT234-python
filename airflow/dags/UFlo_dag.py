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

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'dag_UFlo',
    default_args=default_args,
    description='DAG ETL para Universidad De Flores',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today(),
    catchup=False,
    tags=['example'],
) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load
    

