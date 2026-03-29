"""
from datetime import timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
 
default_args = {
    "owner":            "fantasyland-data",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}
 
with DAG(
    dag_id="fantasyland_transactions_load",
    description="Trigger Databricks to load transactions from S3 → Snowflake RAW",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="5 * * * *",
    catchup=False,
    tags=["fantasyland", "transactions", "databricks"],
) as dag:
 
    load_transactions = DatabricksRunNowOperator(
        task_id="databricks_load_transactions",
        databricks_conn_id="databricks_default",
        job_id="your-databricks-job-id",        # set this after creating job in Databricks UI
        notebook_params={
            "topic": "transactions",
            "date":  "{{ execution_date.strftime('%Y-%m-%d') }}",
            "hour":  "{{ execution_date.strftime('%H') }}",
        },
    )
 """