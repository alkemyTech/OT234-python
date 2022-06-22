"Configurar un DAG, sin consultas, ni procesamiento. Para Hacer un ETL para 2 universidades distintas."

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

# dag configuration
default_args={
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(hours=1)
}

with DAG('Consulta universidades', 
          description='Consulta a la UNiversidad del Comahue',
          scudule_inverval=timedelta(hours=1), 
          star_date=datetime(2022,6,22),
          default_args=default_args,
          template_searchpath='\Users\WalterPc\Documents\OT234-python\include', 
         ) as dag:
         
        UNCo_query=PythonOperator(
            task_id='UNCo_query',
            sql='UNCo_2020-09-01_2021-02-01.sql'
            )

        UNCo_query=PythonOperator(
            task_id='USal_query',
            sql='USal_2020-09-01_2021-02-01'
            )
