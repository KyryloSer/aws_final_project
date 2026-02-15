import os
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3

# Константи (краще брати з оточення, щоб код був універсальним)
DATABASE = 'silver'
BUCKET = os.getenv('DATALAKE_BUCKET', 'dp-feb15-data-lake-157671019517')
IAM_ROLE = os.getenv('REDSHIFT_IAM_ROLE', 'arn:aws:iam::157671019517:role/kirill-data-platform-RedshiftGlueAccessRole-bDB0r7GSUmPa')
WORKGROUP = os.getenv('REDSHIFT_WORKGROUP', 'dp-feb15-workgroup')
REDSHIFT_DB = 'dev'

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def setup_glue_catalog():
    glue = boto3.client('glue', region_name='us-east-1')
    
    try:
        glue.create_database(DatabaseInput={'Name': DATABASE})
    except glue.exceptions.AlreadyExistsException:
        pass
    
    for table in ['customers', 'user_profiles']:
        try:
            glue.delete_table(DatabaseName=DATABASE, Name=table)
        except glue.exceptions.EntityNotFoundException:
            pass

    glue.create_table(
        DatabaseName=DATABASE,
        TableInput={
            'Name': 'customers',
            'TableType': 'EXTERNAL_TABLE',
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'client_id', 'Type': 'int'},
                    {'Name': 'first_name', 'Type': 'string'},
                    {'Name': 'last_name', 'Type': 'string'},
                    {'Name': 'email', 'Type': 'string'},
                    {'Name': 'registration_date', 'Type': 'date'},
                    {'Name': 'state', 'Type': 'string'}
                ],
                'Location': f's3://{BUCKET}/silver/customers/',
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}
            }
        }
    )
    
    glue.create_table(
        DatabaseName=DATABASE,
        TableInput={
            'Name': 'user_profiles',
            'TableType': 'EXTERNAL_TABLE',
            'StorageDescriptor': {
                'Columns': [
                    {'Name': 'email', 'Type': 'string'},
                    {'Name': 'full_name', 'Type': 'string'},
                    {'Name': 'birth_date', 'Type': 'date'},
                    {'Name': 'age', 'Type': 'int'},
                    {'Name': 'state', 'Type': 'string'},
                    {'Name': 'phone_number', 'Type': 'string'}
                ],
                'Location': f's3://{BUCKET}/silver/user_profiles/',
                'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'}
            }
        }
    )

def load_to_redshift():
    redshift = boto3.client('redshift-data', region_name='us-east-1')
    
    commands = [
        "DROP TABLE IF EXISTS gold.user_profiles_enriched;",
        """
        CREATE TABLE gold.user_profiles_enriched (
            client_id INT PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            registration_date DATE,
            state VARCHAR(50),
            birth_date DATE,
            age INT,
            phone_number VARCHAR(50),
            updated_at TIMESTAMP
        );
        """,
        f"COPY gold.user_profiles_enriched FROM 's3://{BUCKET}/gold/user_profiles_enriched/' IAM_ROLE '{IAM_ROLE}' FORMAT AS PARQUET;"
    ]
    
    for sql in commands:
        response = redshift.execute_statement(WorkgroupName=WORKGROUP, Database=REDSHIFT_DB, Sql=sql)
        statement_id = response['Id']
        while True:
            status_resp = redshift.describe_statement(Id=statement_id)
            status = status_resp['Status']
            if status == 'FINISHED':
                break
            elif status in ['FAILED', 'ABORTED']:
                raise Exception(f"Redshift command failed: {status_resp.get('Error')}")
            time.sleep(5)

def run_enrichment():
    glue = boto3.client('glue', region_name='us-east-1')
    job_name = 'dp-feb15-enrichment-silver-to-gold'
    
    response = glue.start_job_run(
        JobName=job_name,
        Arguments={
            '--DATABASE': DATABASE,
            '--CUSTOMERS_TABLE': 'customers',
            '--PROFILES_TABLE': 'user_profiles',
            '--TARGET_S3': f's3://{BUCKET}/gold/user_profiles_enriched/',
        }
    )
    run_id = response['JobRunId']
    while True:
        status_resp = glue.get_job_run(JobName=job_name, RunId=run_id)
        status = status_resp['JobRun']['JobRunState']
        if status == 'SUCCEEDED':
            break
        elif status in ['FAILED', 'STOPPED', 'TIMEOUT']:
            raise Exception(f"Glue Job failed: {status_resp['JobRun'].get('ErrorMessage')}")
        time.sleep(15)

with DAG(
    'enrich_user_profiles',
    default_args=default_args,
    description='Pipeline for Gold layer: Customers + Profiles → Redshift',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['enrichment', 'gold', 'redshift'],
) as dag:

    task1 = PythonOperator(task_id='setup_glue_catalog', python_callable=setup_glue_catalog)
    task2 = PythonOperator(task_id='run_enrichment', python_callable=run_enrichment)
    task3 = PythonOperator(task_id='load_to_redshift', python_callable=load_to_redshift)

    task1 >> task2 >> task3
