from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
import pandas as pd
from helper.minio import MinioClient, CustomMinio
from sqlalchemy import create_engine
from pangres import upsert
from datetime import timedelta
import json
from airflow.models import Variable


class Load:
    @staticmethod
    def _load_minio(table_name, incremental, **kwargs):
            """
            Load data from Minio into staging area.

            Args:
                table_name (str): Name of the table to load data into.
                incremental (bool): Whether to load incremental data or not.
                **kwargs: Additional keyword arguments.
            """
        
            print (f"===========START=========")

            date = kwargs.get('ds')
            table_pkey = eval(Variable.get('tables_to_load'))
            engine = create_engine(PostgresHook(postgres_conn_id='staging-conn').get_uri())
            bucket_name = 'extracted-data'
            print (f"===========Check If Incremental=========")
            if incremental:
                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                object_name = f'/temp/{table_name}.csv'

            print (f"==========={bucket_name}=========")
            
            check_xcom = kwargs['ti'].xcom_pull(
                key=object_name, 
                task_ids=f'extract.extract_{table_name}'
            )
            print(f"===========Check XCOM: {check_xcom}=========")
            if check_xcom is None or check_xcom.get('status') == "skipped":
                raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")
            else:
                df = CustomMinio._get_dataframe(bucket_name, object_name)
                print(df.head())
                df = df.set_index(table_pkey[table_name])
                if table_name == 'aircrafts_data':
                    df['model'] = df['model'].apply(json.dumps)

                if table_name == 'airports_data':
                    df['airport_name'] = df['airport_name'].apply(json.dumps)
                    df['city'] = df['city'].apply(json.dumps)

                if table_name == 'tickets':
                    df['contact_data'] = df['contact_data'].apply(json.dumps)
                
                if table_name == 'flights':
                    df = df.replace({float('nan'): None})

                upsert(
                    con=engine,
                    df=df,
                    table_name=table_name,
                    schema='stg',
                    if_row_exists='update'
                )

    #trial without xcom()
    @staticmethod
    def _load_stg(table_name, incremental, **kwargs):
                """
                Load data from Minio into staging area.

                Args:
                    table_name (str): Name of the table to load data into.
                    incremental (bool): Whether to load incremental data or not.
                    **kwargs: Additional keyword arguments.
                """

                print (f"===========START=========")

                date = kwargs.get('ds')
                table_pkey = eval(Variable.get('tables_to_load'))
                print(table_pkey)
                engine = create_engine(PostgresHook(postgres_conn_id='staging-conn').get_uri())
                bucket_name = 'extracted-data'
                print (f"===========Check If Incremental=========")
                if incremental:
                    object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
                else:
                    object_name = f'/temp/{table_name}.csv'

                print (f"==========={bucket_name}=========")
                print (f"==========={object_name}=========")

    
                df = CustomMinio._get_dataframe(bucket_name, object_name)
                print(df.head())
                df = df.set_index(table_pkey[table_name])
                if table_name == 'aircrafts_data':
                    df['model'] = df['model'].apply(json.dumps)

                upsert(
                    con=engine,
                    df=df,
                    table_name=table_name,
                    schema='stg',
                    if_row_exists='update'
                )



