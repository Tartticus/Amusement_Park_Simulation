
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
 
default_args = {
    "owner":            "fantasyland-data",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}
 
LOADER = "/opt/airflow/project/python_etl/s3_bucket_copy.py"
 
with DAG(
    dag_id="fantasyland_transactions_load",
    description="Load transaction events from S3 into Snowflake RAW hourly",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="5 * * * *",
    catchup=False,
    tags=["fantasyland", "transactions", "ingestion"],
) as dag:
 
    load_transactions = BashOperator(
        task_id="load_transactions_from_s3",
        bash_command=(
            "python {{ params.loader }} transactions "
            "{{ execution_date.strftime('%Y-%m-%d') }} "
            "{{ execution_date.strftime('%H') }}"
        ),
        params={"loader": LOADER},
    )