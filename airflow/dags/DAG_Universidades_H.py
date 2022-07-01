from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import logging
import sys

"""
The following are possible operators to be used for working with Python scripts, PostgreSQL and Amazon S3:

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3CreateObjectOperator

"""

#############################
# Setting up logging tool
logger = logging.getLogger("LOG-Universidades_H")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("Universidades_H.log")
stream_handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(asctime)s] %(name)s: %(message)s", datefmt='%m/%d/%Y')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)
#############################

logger.info("Iniciando DAG...")

with DAG(
    "DAG_Universidades_H",       #DAG Name
    description="Configuracion basica del DAG",
    schedule_interval=timedelta(hours=1),   # Execute every 1 hour, every day
    start_date=datetime(2022, 6, 21),
    default_args={"retries": 5}         # Set 5 retries as limit 
) as dag:

    UCin = EmptyOperator(task_id="Universidad_del_Cine", retries=2)   # Can also define amount of retries for each task
    UBAi = EmptyOperator(task_id="Universidad_de_Buenos_Aires")
    tarea_ejemplo_1 = EmptyOperator(task_id="Tarea_1")
    tarea_ejemplo_2 = EmptyOperator(task_id="Tarea_2")

    UCin >> tarea_ejemplo_1
    UBAi >> tarea_ejemplo_2