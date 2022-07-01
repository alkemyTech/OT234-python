import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import csv

'''
sql_query : absolute path to sql query file (ex: '../airflow/Include/ ...sql')
raw_path : absolute path to raw  (ex: '../airflow/files/')
Univ: University (ex: 'UPal')
'''

def queryTOcsv(Univ, raw_path, sql_query):

    postgres_conn_id='postgres_default'

    pg_hook = PostgresHook.get_hook(postgres_conn_id)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Open and read the file as a single buffer
    fd = open(sql_query, 'r')
    sqlFile = fd.read()
    fd.close()

    # Execute the sql script 
    try:
        cursor.execute(sqlFile)
        result = cursor.fetchall()
    except:
        print("sql Command error. Query skipped")

    # Write to CSV file
    csv_path = raw_path + Univ + '_dump.csv'
    with open(csv_path, 'w', encoding="ISO-8859-1") as fp:
        a = csv.writer(fp, delimiter = ',')
        a.writerow([i[0] for i in cursor.description])
        a.writerows(result)
   
    fp.close()
    cursor.close()