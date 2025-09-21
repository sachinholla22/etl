# etl/extract.py

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import os
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def download_from_s3(bucket, object_name, local_file_path):
    """
    Downloads a file from S3 if it doesn't already exist locally.
    """
    if os.path.exists(local_file_path):
        logging.info(f"File already exists: {local_file_path}")
        return local_file_path

    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, local_file_path)
        logging.info(f"File downloaded successfully: {local_file_path}")
        return local_file_path
    except FileNotFoundError:
        logging.error("Local file path does not exist.")
        return None
    except NoCredentialsError:
        logging.error("AWS credentials not found.")
        return None
    except ClientError as e:
        logging.error(f"AWS Client Error: {e}")
        return None

def list_csv_files(folder_path):
    """
    Returns a list of .csv files in the given folder.
    """
    return [f for f in os.listdir(folder_path) if f.endswith(".csv")]

def find_csv_file(csv_files, target_name):
    """
    Finds a specific CSV file by exact name.
    """
    for file in csv_files:
        if file == target_name:
            return file
    return None
