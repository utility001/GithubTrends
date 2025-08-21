# Import
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.etl import github_api_to_s3, s3_to_rds, transform_and_upload_data
from utils.rds import create_table_on_rds

# DAG timezone configuration
UTC_TIMEZONE = pendulum.timezone("UTC")

# Default arguments block
default_args = {
    "owner": "Data Engineering Team",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=3),
}

# Dag definition block
dag = DAG(
    dag_id="github-trends-pipeline",
    description="Gets data from github API - tranform - Loads into RDS",
    schedule="@daily",
    start_date=pendulum.datetime(year=2025, month=8, day=1, tz=UTC_TIMEZONE),
    default_args=default_args,
    catchup=False,
)

task_1_api_to_s3 = PythonOperator(
    task_id="github-api-to-s3",
    python_callable=github_api_to_s3,
    dag=dag,
)

task_2_transform = PythonOperator(
    task_id="s3-data-transform-and-reupload",
    python_callable=transform_and_upload_data,
    dag=dag,
)

task_3_create_table = PythonOperator(
    task_id="create-table-if-not-exists",
    python_callable=create_table_on_rds,
    dag=dag,
)

task_4_s3_to_rds = PythonOperator(
    task_id="s3-to-rds",
    python_callable=s3_to_rds,
    dag=dag,
)

task_1_api_to_s3 >> task_2_transform >> task_3_create_table >> task_4_s3_to_rds
