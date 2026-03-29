

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
 
default_args = {
    "owner":            "fantasyland-data",
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
}
 
DBT_DIR = "/opt/airflow/project/dbt"
 
with DAG(
    dag_id="fantasyland_dbt_silver",
    description="Run dbt silver models hourly — RAW to CORE",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="15 * * * *",   # :15 past — after loads at :05
    catchup=False,
    tags=["fantasyland", "dbt", "silver"],
) as dag:
 
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select silver.*",
    )
 
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select silver.*",
    )
 
    dbt_silver >> dbt_test_silver
 