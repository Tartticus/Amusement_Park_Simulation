"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
 
#default_args = {
    "owner":            "fantasyland-data",
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}
 
#SEED_SCRIPT = "/opt/airflow/project/seed/seed_guests.py"
 
#with DAG(
    dag_id="fantasyland_guests_refresh",
    description="Refresh guest pool in Snowflake CORE.GUESTS daily",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",    # 2am UTC every day
    catchup=False,
    tags=["fantasyland", "guests", "daily"],
)# as dag:
 
    refresh_guests = BashOperator(
        task_id="refresh_guests",
        bash_command=f"python {SEED_SCRIPT}",
    )
 """