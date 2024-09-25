import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta

dag = DAG(
    dag_id = "brewery_list_etl",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

run_bronze = SparkSubmitOperator(
    task_id="run_bronze_ingestion",
    conn_id="spark-con",
    application='/opt/airflow/jobs/bronze_ingestion.py',
    retries=3,
    retry_delay=timedelta(minutes=1),
    dag=dag
)

run_bronze_validation = SparkSubmitOperator(
    task_id='run_bronze_validation',
    conn_id="spark-con",
    application='/opt/airflow/jobs/bronze_validation.py',
    conf={"spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0"},
    dag=dag
)

run_silver = SparkSubmitOperator(
    task_id="run_silver_ingestion",
    conn_id="spark-con",
    application='/opt/airflow/jobs/silver_ingestion.py',
    conf={"spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0"},
    dag=dag
)

run_silver_validation = SparkSubmitOperator(
    task_id='run_silver_validation',
    conn_id="spark-con",
    application='/opt/airflow/jobs/silver_validation.py',
    conf={"spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0"},
    dag=dag
)

run_gold = SparkSubmitOperator(
    task_id="run_gold_ingestion",
    conn_id="spark-con",
    application='/opt/airflow/jobs/gold_ingestion.py',
    conf={"spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0"},
    dag=dag
)


run_bronze >> run_bronze_validation >> run_silver >> run_silver_validation >> run_gold