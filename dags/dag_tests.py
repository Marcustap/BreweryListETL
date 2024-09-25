from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    dag_id='run_tests_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task que roda os testes
    run_tests = BashOperator(
        task_id='run_unit_tests',
        bash_command='python /opt/airflow/tests/run_tests.py'
    )

    run_tests