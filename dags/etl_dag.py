from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/Documents/study/my task/projrct/ETL-PIPELINE/scripts")

from extract import extract_from_postgres
from transform import transform_data
from load import upload_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_sales_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data using Airflow',
    schedule_interval=timedelta(days=1),
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_from_postgres,
    op_kwargs={'table_name': 'sales_data', 'output_csv': 'data/raw_sales_data.csv'},
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=upload_to_s3,
    op_kwargs={'file_path': 'data/processed_sales_data.csv', 'bucket': 'your-s3-bucket', 's3_filename': 'processed_sales_data.csv'},
    dag=dag,
)

extract_task >> transform_task >> load_task
