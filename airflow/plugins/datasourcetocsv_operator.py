from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
from os import environ
import csv

class DataSourceToCsvOperator(BaseOperator):
    """
    Extract data from the data source to CSV file
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    def __init__(
            self, sql,
            postgres_conn_id='postgres_default', autocommit=False,
            parameters=None,
            database=None,
            output_dir='/tmp/',
            *args, **kwargs):
        super(DataSourceToCsvOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.file_path = output_dir

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id,
                                 schema=self.database)
        conn = self.hook.get_conn()
        cursor = conn.cursor()
        #self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        cursor.execute(self.sql)
        result = cursor.fetchall()
        # Write to CSV file
        # temp_path = self.file_path + '_dump_.csv'
        # tmp_path = self.file_path + 'dump.csv'
        temp_path = self.file_path
        tmp_path = self.file_path 
        print(temp_path,tmp_path)

        with open(temp_path, 'w') as fp:
            a = csv.writer(fp, quoting = csv.QUOTE_MINIMAL, delimiter = '|')

            a.writerow([i[0] for i in cursor.description])
            a.writerows(result)
        #full_path = temp_path + '.gz'
        with open(temp_path, 'rb') as f:
            data = f.read()
        f.close()
        # self.hook.bulk_dump(self.sql,tmp_path)