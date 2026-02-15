#!/usr/bin/env python3
"""
Setup Glue Catalog tables for Silver layer
"""
import boto3
import sys

glue = boto3.client('glue', region_name='us-east-1')

DATABASE = 'silver'
BUCKET = 'dp-feb15-data-lake-157671019517'

def create_database():
    """Create Glue Database if not exists"""
    try:
        glue.create_database(
            DatabaseInput={'Name': DATABASE}
        )
        print(f"✅ Database '{DATABASE}' created")
    except glue.exceptions.AlreadyExistsException:
        print(f"✅ Database '{DATABASE}' already exists")

def create_table(name, columns, location):
    """Create Glue Table if not exists"""
    try:
        glue.create_table(
            DatabaseName=DATABASE,
            TableInput={
                'Name': name,
                'StorageDescriptor': {
                    'Columns': columns,
                    'Location': location,
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                }
            }
        )
        print(f"✅ Table '{name}' created")
    except glue.exceptions.AlreadyExistsException:
        print(f"✅ Table '{name}' already exists")

def main():
    # Create database
    create_database()
    
    # Create customers table
    create_table(
        name='customers',
        columns=[
            {'Name': 'client_id', 'Type': 'int'},
            {'Name': 'first_name', 'Type': 'string'},
            {'Name': 'last_name', 'Type': 'string'},
            {'Name': 'email', 'Type': 'string'},
            {'Name': 'registration_date', 'Type': 'date'},
            {'Name': 'state', 'Type': 'string'}
        ],
        location=f's3://{BUCKET}/silver/customers/'
    )
    
    # Create user_profiles table
    create_table(
        name='user_profiles',
        columns=[
            {'Name': 'email', 'Type': 'string'},
            {'Name': 'full_name', 'Type': 'string'},
            {'Name': 'age', 'Type': 'int'},
            {'Name': 'state', 'Type': 'string'},
            {'Name': 'phone_number', 'Type': 'string'}
        ],
        location=f's3://{BUCKET}/silver/user_profiles/'
    )
    
    print("\n🎉 Glue Catalog setup complete!")

if __name__ == '__main__':
    main()
