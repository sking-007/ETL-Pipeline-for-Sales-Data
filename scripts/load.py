import boto3
import os

# AWS Credentials
AWS_ACCESS_KEY = "your_access_key"
AWS_SECRET_KEY = "your_secret_key"
BUCKET_NAME = "your-s3-bucket-name"

def upload_to_s3(file_path, bucket, s3_filename):
    try:
        s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
        s3.upload_file(file_path, bucket, s3_filename)
        print(f"Successfully uploaded {file_path} to {bucket}/{s3_filename}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

if __name__ == "__main__":
    processed_file = "data/processed_sales_data.csv"
    s3_filename = "processed_sales_data.csv"
    
    upload_to_s3(processed_file, BUCKET_NAME, s3_filename)
