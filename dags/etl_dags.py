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

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    schedule="@daily",
    description="my first airflow project",
    catchup=False 
) as dag:
    extract_task=PythonOperator(
        task_id="extracting",
        python_callable=download_from_s3
    )
    
    transform_task=PythonOperator(
        task_id="transforming",
        python_callable=transform_csv
    )
    load_task=PythonOperator(
        task_id="loading",
        python_callable=load_cleaned_file
    )

    extract_task >> transform_task >>load_task