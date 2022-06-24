from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

import psycopg2


def connectiondb():

    try:
        connect = psycopg2.connect("dbname='training' user='alkymer' password='alkymer123' host='training-main.cghe7e6sfljt.us-east-1.rds.amazonaws.com'")
    
    except psycopg2.OperationalError as err:
        print('NO fue posible hacer la coneccion')
        print(err)



default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}


with DAG('Reintiento de coneccion a Base de datos', 
          description='Consulta a la UNiversidad del Comahue',
          scudule_inverval=timedelta(hours=1), 
          star_date=datetime(2022,6,22),
          default_args=default_args,
          ) as dag:

    conn_db= PythonOperator(
            tassk_id='Coneccion a DB'
            dag=dag
            python_callable=connectiondb
            )
    conn_db

