

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
    dag_id="fantasyland_dbt_gold",
    description="Run dbt gold models hourly — CORE to ANALYTICS",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="30 * * * *",   # :30 past — after silver at :15
    catchup=False,
    tags=["fantasyland", "dbt", "gold"],
) as dag:
 
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select gold.*",
    )
 
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && dbt test --select gold.*",
    )
 
    dbt_gold >> dbt_test_gold
 