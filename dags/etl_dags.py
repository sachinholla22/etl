from airflow import DAG
import datetime
from etl.extract import download_from_s3
from etl.load import load_cleaned_file
from etl.transform import transform_csv
from airflow.operators.python import PythonOperator

default_args={
    "start_date":datetime.datetime(2025,9,29),
    "retries":1
}

def extract_file_from_s3(**kwargs):
    bucket_name = "my-bucket-2001-test"
    s3_key = "hospital/patients.csv"
    local_file_path = "/Data/patients.csv"

    downloaded = download_from_s3(bucket_name, s3_key, local_file_path)
    kwargs['ti'].xcom_push(key="downloaded_file",value=downloaded)

def transform_file(**kwargs):
    ti=kwargs['ti']
    dowloaded_file=ti.xcom_pull(task_ids='extracting',key='downloaded_file')
    cleaned_file=transform_csv(dowloaded_file)    
    ti.xcom_push(key='cleaned_file',value=cleaned_file)

def load_file(**kwargs):
    ti=kwargs['ti']
    cleaned_file=ti.xcom_pull(task_ids='transforrming',key='cleaned_file')
    bucket_name = "my-bucket-2001-test"
    
    # Call the load_cleaned_file function to load the cleaned file to S3
    res = load_cleaned_file(cleaned_file, bucket_name)

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    schedule="@daily",
    description="my first airflow project",
    catchup=False 
) as dag:
    extract_task=PythonOperator(
        task_id="extracting",
        python_callable=extract_file_from_s3,
        
    )
    
    transform_task=PythonOperator(
        task_id="transforming",
        python_callable=transform_file,
    
    )
    load_task=PythonOperator(
        task_id="loading",
        python_callable=load_file,
        
    )

    extract_task >> transform_task >>load_task