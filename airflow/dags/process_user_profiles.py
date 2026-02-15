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
    'process_user_profiles',
    default_args=default_args,
    description='User Profiles pipeline: Raw → Bronze → Silver (manual trigger)',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['user_profiles', 'etl', 'bronze', 'silver', 'manual'],
) as dag:

    # Task 1: Raw → Bronze
    raw_to_bronze = GlueJobOperator(
        task_id='user_profiles_raw_to_bronze',
        job_name='dp-feb15-user-profiles-raw-to-bronze',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Task 2: Bronze → Silver
    bronze_to_silver = GlueJobOperator(
        task_id='user_profiles_bronze_to_silver',
        job_name='dp-feb15-user-profiles-bronze-to-silver',
        region_name='us-east-1',
        wait_for_completion=True,
        verbose=True,
    )

    # Dependencies
    raw_to_bronze >> bronze_to_silver
