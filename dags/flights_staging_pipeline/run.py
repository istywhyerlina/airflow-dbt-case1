from airflow.decorators import dag, task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime
from helper.minio import MinioClient
from helper.read_sql import read_sql_file
from helper.callbacks.slack_notifier import slack_notifier
from flights_staging_pipeline.tasks.staging.components.extract import Extract
from flights_staging_pipeline.tasks.staging.components.Load import Load
from airflow.models import Variable
import json
from airflow.exceptions import AirflowSkipException
from datetime import timedelta
from airflow.models.taskinstance import TaskInstance
import pendulum




default_args = {
    'on_failure_callback': slack_notifier
}

incremental = eval(Variable.get('incremental'))
#tables_to_extract= ['aircrafts_data', 'airports_data', 'bookings', 'tickets', 'seats', 'flights', 'ticket_flights', 'boarding_passes']
tables_to_extract=eval(Variable.get('tables_to_extract'))
tables_to_transform=eval(Variable.get('tables_to_transform'))
tables_to_load = eval(Variable.get('tables_to_load'))
keys = list(tables_to_load.keys())


@dag(
    dag_id = 'flights_staging_pipeline',
    start_date = datetime.strptime('2025-01-01', '%Y-%m-%d'),
    schedule = "@daily",
    catchup = True,
    max_active_runs = 1,
    default_args=default_args
)
def flights_staging_pipeline():
    @task
    def create_bucket():
        minio_client = MinioClient._get()
        bucket_name = 'extracted-data'
        
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)

    @task_group
    def extract():
        for table_name in tables_to_extract:
            current_task = PythonOperator(
                task_id = f'extract_{table_name}',
                python_callable = Extract._extract_src,
                op_kwargs={'table_name': table_name, 'incremental': incremental}
                )
            current_task


    @task_group
    def load():
        previous_task = None
        for table_name in keys:
            current_task = PythonOperator(
                task_id = f'load_{table_name}',
                python_callable = Load._load_minio,
                trigger_rule='none_failed',
                op_kwargs={'table_name': table_name, 'incremental': incremental}

            )
            if previous_task:
                    previous_task >> current_task
        
            previous_task = current_task

    trigger_warehouse= TriggerDagRunOperator(task_id='trigger_flights_warehouse_pipeline',trigger_dag_id='flights_warehouse_pipeline',wait_for_completion=True,trigger_rule='none_failed')
         
    create_bucket() >> extract() >> load() >> trigger_warehouse

flights_staging_pipeline()