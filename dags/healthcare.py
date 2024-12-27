from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dbt_healthcare_dag',
    default_args=default_args,
    description='A DAG to run dbt models for healthcare project',
    schedule_interval=timedelta(days=1),
)

# Task to run the silver model
dbt_run_silver = BashOperator(
    task_id='dbt_run_silver',
    bash_command='dbt run --models silver_healthcare_data --profiles-dir /usr/local/airflow/dags/dbt_project --project-dir /usr/local/airflow/dags/dbt_project',
    dag=dag,
)

# Task to run the gold model
dbt_run_gold = BashOperator(
    task_id='dbt_run_gold',
    bash_command='dbt run --models gold_healthcare_diagnosis_summary --profiles-dir /usr/local/airflow/dags/dbt_project --project-dir /usr/local/airflow/dags/dbt_project',
    dag=dag,
)

# Set task dependencies (dbt_run_silver runs before dbt_run_gold)
dbt_run_silver >> dbt_run_gold
