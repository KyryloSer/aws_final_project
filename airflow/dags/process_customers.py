from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_customers',
    default_args=default_args,
    description='Customers pipeline: Raw → Bronze → Silver (with deduplication)',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['customers', 'etl', 'bronze', 'silver'],
) as dag:

    # Task 1: Raw → Bronze
    raw_to_bronze = GlueJobOperator(
        task_id='customers_raw_to_bronze',
        job_name='dp-feb15-customers-raw-to-bronze',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Task 2: Bronze → Silver (includes deduplication)
    bronze_to_silver = GlueJobOperator(
        task_id='customers_bronze_to_silver',
        job_name='dp-feb15-customers-bronze-to-silver',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Dependencies
    raw_to_bronze >> bronze_to_silver
