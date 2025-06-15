from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow import AirflowException
from helper.minio import MinioClient
from helper.minio import CustomMinio
from datetime import timedelta
from airflow.models import Variable

import pandas as pd
import requests

BASE_PATH = "/opt/airflow/dags"


class Extract:
    @staticmethod
    def _extract_src(table_name, incremental, **kwargs):
        """
        Extract data from source database.

        Args:
            table_name (str): Name of the table to extract data from.
            incremental (bool): Whether to extract incremental data or not.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowException: If failed to extract data from source database.
            AirflowSkipException: If no new data is found.
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id='sources-conn')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            query = f"SELECT * FROM bookings.{table_name}"
            if incremental:
                date = kwargs['ds']
                query += f" WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';"

                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            
            else:
                object_name = f'/temp/{table_name}.csv'

            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            connection.commit()
            connection.close()

            column_list = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(result, columns=column_list)

            if incremental:
                if df.empty:
                    kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "skipped" })
                    raise AirflowSkipException(f"{table_name} doesn't have new data. Skipped...")
                else:
                    kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "success" })
            else:
                kwargs['ti'].xcom_push(key=object_name, value={'data':table_name,'status': "success" })


            bucket_name = 'extracted-data'

            CustomMinio._put_csv(df, bucket_name, object_name)

        except AirflowSkipException as e:
            raise e
        except Exception as e:
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")

