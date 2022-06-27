# Airflow imports 
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datasourcetocsv_operator import DataSourceToCsvOperator
# ----------------

# Numpy and pandas 
import pandas as pd
import csv
import numpy as np
# ----------------

# Time Module
from datetime import datetime, timedelta
now = datetime.now()
current_time = now.strftime("%d/%m:%y")

# ----------------

# Global Variables

dataset_dir = "/home/jvera/gitRepos/OT234-python/airflow/datasets/"
sql_dir = "/home/jvera/gitRepos/OT234-python/airflow/include/"
csv_UFLo_dir = "/home/jvera/gitRepos/OT234-python/airflow/datasets/UFLo_2020-09-01_2021-02-01.csv"
csv_UTNa_dir = "/home/jvera/gitRepos/OT234-python/airflow/datasets/UTNa_2020-09-01_2021-02-01.csv"
txt_UFLo_dir = "/home/jvera/gitRepos/OT234-python/airflow/files/UFLo_2020-09-01_2021-02-01.txt"
txt_UTNa_dir = "/home/jvera/gitRepos/OT234-python/airflow/files/UTNa_2020-09-01_2021-02-01.txt"
# ----------------


def sqlFileToQuery(path):
    query = ''
    with open(path) as file:
        for line in file:
            query += line
    return query

def cpDict(path):
    with open(path) as file:
        reader = csv.reader(file)
        cpDict = {rows[0]:rows[1] for rows in reader}
        
    return cpDict

def BaseProcessing(df):
    
    df.university = df.university.str.replace('_',' ')
    df.career = df.career.str.replace('_',' ')
    df.inscription_date = df.inscription_date.str.replace('/','-')
    df.first_name = df.first_name.str.replace(' ','').str.replace('-','').str.upper()
    df.last_name = df.last_name.str.replace(' ','').str.replace('-','').str.upper()
    df.gender = df.gender.str.replace('m','male').replace('f','female')
    df.age = df.age.apply(lambda x: int(x.split(' ')[0])//365).astype(int)
    df.age = df.age.apply(lambda x: x if x > 0 else x + 100)
    if df.postal_code.dtypes != 'int':
        df.postal_code = df.postal_code.apply(lambda x: int(x.split(' ')[-1]))
    
    df.email = df.email.str.replace(' ','').str.replace('-','').str.lower()
    return df

def postalCode(df):
    cp_dict = cpDict("/home/jvera/gitRepos/OT234-python/airflow/plugins/codigos_postales.csv")
    df.location = df.postal_code.apply(lambda x : cp_dict[str(x)]).str.lower()
    return df

def exportToTxt(df,path):
    print(path)
    print('HOLA')
    print(df.head())
    
    np.savetxt(path, df.values, fmt='%s',delimiter=',')

def cleaningPipeline(**kwargs):

    input_path = kwargs['input_path']
    postal_fix = kwargs['postal_fix']
    output_path = kwargs['output_path']

    df = pd.read_csv(input_path,sep='|')

    df = BaseProcessing(df)
    if postal_fix == True:
        df = postalCode(df)
    print('hola')
    exportToTxt(df,output_path)


default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(hours=1)
}

with DAG(
    'UFLo_UTNa',
    description='dag para la univesidad UNTF',
    schedule_interval=timedelta(hours=20),
    start_date=datetime(2022,6,27),
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

    UFLo_transform_to_txt = PythonOperator(
        task_id='UFLo_transform_to_txt',
        python_callable=cleaningPipeline,
        op_kwargs={'input_path': csv_UFLo_dir, 'postal_fix': 'True', 'output_path' : txt_UFLo_dir},
        )

    UTNa_query = DataSourceToCsvOperator(
        task_id='UTNa_query',
        sql=sqlFileToQuery(sql_dir + "UTNa_2020-09-01_2021-02-01_OT234-12.sql"),
        postgres_conn_id="postgres_server",
        database="training",
        output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01.csv'
        # output_dir= dataset_dir + 'UTNa_2020-09-01_2021-02-01-{}.csv'.format(current_time)
        )

    UTNa_transform_to_txt = PythonOperator(
        task_id='UTNa_transform_to_txt',
        python_callable=cleaningPipeline,
        op_kwargs={'input_path': csv_UTNa_dir, 'postal_fix': 'False','output_path':txt_UTNa_dir},
        )

UFLo_query >> UFLo_transform_to_txt

UTNa_query >> UTNa_transform_to_txt
    
   
    
    