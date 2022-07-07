import dagfactory
from airflow import DAG
from airflow.operators.python import PythonOperator
from DAGs_functions import extract_from_db, transform_extrated_data, load_to_s3
from datetime import datetime
from pathlib import Path

FILE_PATH = Path(__file__).parent.absolute().parent

universitties = ['UAIn', 'UNLP']
args = ['sql_name','raw_data_name', 'dataset_name']
files_names = ['_2020-09-01_2021-02-01_OT234-16.sql', '_raw_data.csv', '_dataset.csv']
kwargs = [{},{}]

# This loop sets up all args needed for each PythonOperator
for n, name in enumerate(universitties):
    for m, arg in enumerate(args):
        kwargs[n][arg] = name + files_names[m] 

# [
# {'sql_name': 'UAIn_2020-09-01_2021-02-01_OT234-16.sql',
#   'raw_data_name': 'UAIn_raw_data.csv',
#   'dataset_name': 'UAIn_dataset.csv'},
#
#  {'sql_name': 'UNLP_2020-09-01_2021-02-01_OT234-16.sql',
#   'raw_data_name': 'UNLP_raw_data.csv',
#   'dataset_name': 'UNLP_dataset.csv'}
# ]

dag_factory = dagfactory.DagFactory(FILE_PATH.joinpath('dags').joinpath('config_file.yml'))
#dag_factory.clean_dags(globals())

# Creating Dags dictornaries to be loaded in globals() according to each university
dags ={}
for n, name in enumerate(universitties):
    dags['DAG_' + name] = DAG(dag_id='DAG_' + name, default_args=dag_factory.get_default_config(), start_date=datetime(2022, 6, 18))
    # Tasks
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_from_db,
        op_kwargs=kwargs[n],
        dag = dags['DAG_' + name]
        )
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_extrated_data,
        op_kwargs=kwargs[n],
        dag = dags['DAG_' + name]
        )
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_s3,
        op_kwargs=kwargs[n],
        dag = dags['DAG_' + name]
        )
    # Dependencies
    extract_data >> transform_data >> load_data

# registering dags on globals()
dag_factory.register_dags(dags, globals=globals())
