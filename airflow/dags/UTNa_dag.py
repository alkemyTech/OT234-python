# Airflow imports 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datasourcetocsv_operator import DataSourceToCsvOperator
# ----------------

# Custom Libraries
import customLibs as lib
# ----------------

# Time Module
from datetime import datetime, timedelta
now = datetime.now()
current_time = now.strftime("%d/%m:%y")
# ----------------

# Global Variables
from pathlib import Path
path = str((Path(__file__).parent.absolute()).parent)

dataset_dir = path + "/datasets/"
sql_dir = path + "/include/"
csv_UNTF_dir = path + "/datasets/UNTF_2020-09-01_2021-02-01.csv"
csv_UTNa_dir = path + "/datasets/UTNa_2020-09-01_2021-02-01.csv"
txt_UNTF_dir = path + "/files/UNTF_2020-09-01_2021-02-01.txt"
txt_UTNa_dir = path + "/files/UTNa_2020-09-01_2021-02-01.txt"
aws_bucket = 'cohorte-junio-a192d78b'
# ----------------

default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(hours=1)
}

with DAG(
    'UTNa',
    description='dag para la univesidad UTNa',
    schedule_interval=timedelta(hours=20),
    start_date=datetime(2022,7,6),
    default_args=default_args,

) as dag:

    UTNa_query = DataSourceToCsvOperator(
        task_id='UTNa_query',
        sql=lib.sqlFileToQuery(sql_dir + "UTNa_2020-09-01_2021-02-01_OT234-12.sql"),
        postgres_conn_id="postgres_server",
        database="training",
        output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01.csv'
        # output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01-{}.csv'.format(current_time)
        )

    UTNa_transform_to_txt = PythonOperator(
        task_id='UTNa_transform_to_txt',
        python_callable=lib.cleaningPipeline,
        op_kwargs={'input_path': csv_UTNa_dir, 'postal_fix': 'False','output_path':txt_UTNa_dir},
        )

    UTNa_aws_load = PythonOperator(
        task_id='UTNa_aws_load',
        python_callable=lib.loadAws,
        op_kwargs={
            'bucket': aws_bucket, 
            'file_path': txt_UTNa_dir,
        }
    )


UTNa_query >> UTNa_transform_to_txt >> UTNa_aws_load
    
   
    
    