"""
https://intuitivedataguide.medium.com/building-a-simple-etl-with-airflow-postgresql-and-docker-a2b1a2b202ec
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_test",
    start_date=datetime(2024, 9, 11),
    schedule_interval=None,
    tags=["test"],
) as dag:
    BashOperator(
        task_id="run_cmd",
        bash_command="python --version",
    )
