"""
Upload generated CSV files to MinIO (Bronze source bucket)
This simulates source system extracts landing in S3
"""

import boto3
from botocore.client import Config
import os

# MinIO connection
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='admin123',
    config=Config(signature_version='s3v4')
)

# Files to upload
files = [
    'data/sample/customers.csv',
    'data/sample/accounts.csv',
    'data/sample/transactions.csv',
    'data/sample/aml_alerts.csv'
]

bucket = 'source'

print("=" * 80)
print("UPLOADING SOURCE DATA TO MinIO")
print("=" * 80)

for file_path in files:
    filename = os.path.basename(file_path)
    s3_key = f"banking/{filename}"
    
    try:
        s3_client.upload_file(file_path, bucket, s3_key)
        print(f"✅ Uploaded: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Error uploading {filename}: {str(e)}")

print("\n" + "=" * 80)
print("✅ UPLOAD COMPLETE!")
print("=" * 80)
print(f"\nVerify in MinIO UI: http://localhost:9001")
print(f"Bucket: {bucket}")
print(f"Path: banking/")