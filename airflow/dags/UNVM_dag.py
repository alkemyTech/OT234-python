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
# dummyOperator: oportunamente deberá ser reemplazado
from airflow.operators.dummy import DummyOperator

with DAG(
    'dag_UNVM',
    description='DAG ETL para Universidad Nacional De Villa María',
    schedule_interval=timedelta(hours=1),
    start_date=datetime.today()
) as dag:

    extract= DummyOperator(task_id='extract')
    transform= DummyOperator(task_id='transform')
    load= DummyOperator(task_id='load')

    extract >> transform >> load
