import json
import boto3
import pandas as pd
import io
import pyarrow
from datetime import datetime

def lambda_handler(event, context):
    # --- 1. Get bucket and file info ---
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_name = event['Records'][0]['s3']['object']['key']
    
    # --- 2. Read the JSON file from S3 ---
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_name)
    content = response['Body'].read().decode('utf-8')  
    df = pd.read_json(content)

    # --- 3. Normalize column names: lowercase + replace spaces/hyphens with underscores ---
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r'[\s\-]+', '_', regex=True)
    )

    # --- 4. Convert date columns to date32[day][pyarrow] ---
    date_columns = ['order_date', 'ship_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d', errors='coerce').dt.date
            df = df.astype({col: 'date32[day][pyarrow]'})

    # --- 5. Handle other column types ---
    if 'returns' in df.columns:
        df['returns'] = df['returns'].astype(str)

    for col in ['ind1', 'ind2']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    # --- 6. Write to in-memory Parquet using PyArrow ---
    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer,
        index=False,
        engine='pyarrow',
        coerce_timestamps='ms',  # in case you have timestamp columns
        flavor='spark'
    )

    # --- 7. Upload Parquet file to S3 ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_base = file_name.split('/')[-1].replace('.json', '')
    output_key = f'outputfolder/{file_base}_{timestamp}.parquet'
    s3.put_object(Bucket=bucket_name, Key=output_key, Body=parquet_buffer.getvalue())

    # --- 8. Trigger Glue Crawler ---
    glue = boto3.client('glue')
    glue.start_crawler(Name='superstore_etl_pipeline_crawler')

    # --- 9. Return success response ---
    return {
        'statusCode': 200,
        'body': json.dumps('Parquet file created and Glue crawler triggered.')
    }
