import boto3
import time
import sys

redshift = boto3.client('redshift-data', region_name='us-east-1')
WORKGROUP = 'dp-feb15-workgroup'
DATABASE = 'dev'
IAM_ROLE = 'arn:aws:iam::157671019517:role/kirill-data-platform-RedshiftGlueAccessRole-bDB0r7GSUmPa'
BUCKET = 'dp-feb15-data-lake-157671019517'

def wait_for_statement(statement_id):
    while True:
        status_resp = redshift.describe_statement(Id=statement_id)
        status = status_resp['Status']
        if status == 'FINISHED':
            return True
        elif status in ['FAILED', 'ABORTED']:
            return False
        time.sleep(1)

def main():
    # 1. Prepare table
    sql = "CREATE SCHEMA IF NOT EXISTS gold; DROP TABLE IF EXISTS gold.sales; CREATE TABLE gold.sales (product_name VARCHAR(512), client_id INT, price DECIMAL(12,2), purchase_date DATE);"
    resp = redshift.execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sql=sql)
    wait_for_statement(resp['Id'])

    # 2. Load partitions
    for day in range(1, 11):
        date_str = f'2022-09-{day:02d}'
        copy_sql = f"""
        BEGIN;
        CREATE TEMP TABLE sales_staging (product_name VARCHAR(512), client_id INT, price DECIMAL(12,2));
        COPY sales_staging FROM 's3://{BUCKET}/silver/sales/purchase_date={date_str}/' IAM_ROLE '{IAM_ROLE}' FORMAT AS PARQUET;
        INSERT INTO gold.sales SELECT product_name, client_id, price, CAST('{date_str}' AS DATE) FROM sales_staging;
        DROP TABLE sales_staging;
        COMMIT;
        """
        resp = redshift.execute_statement(WorkgroupName=WORKGROUP, Database=DATABASE, Sql=copy_sql)
        wait_for_statement(resp['Id'])

    print("✅ gold.sales loaded successfully")

if __name__ == "__main__":
    main()
