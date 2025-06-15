from datetime import datetime
from airflow.datasets import Dataset
from helper.callbacks.slack_notifier import slack_notifier

from cosmos.config import ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping
from cosmos import DbtDag

import os

DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dags/flights_warehouse_pipeline/warehouse_dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="datawarehouse"
)

profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="warehouse",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='warehouse-conn',
        profile_args={"schema": "warehouse"}
    )
)

dag = DbtDag(
    dag_id="flights_warehouse_pipeline",
    # schedule=[
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.dim_aircrafts"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.dim_airport"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.dim_passenger"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.dim_seat"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.fct_boarding_pass"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.fct_booking_ticket"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.fct_flight_activity"),
    #     Dataset("postgres://warehouse-db:5432/warehouse.stg.fct_seat_occupied_daily")
    # ],
    schedule=None,
    catchup=False,
    start_date=datetime(2024, 1, 1),
    project_config=project_config,
    profile_config=profile_config,
    render_config=RenderConfig(
        dbt_executable_path="/opt/airflow/dbt_venv/bin",
        emit_datasets=True
    ),
    default_args={
        'on_failure_callback': slack_notifier
    }
)