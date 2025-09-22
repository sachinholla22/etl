import pandas as pd 
import boto3
import logging
from botocore.exceptions import ClientError
import os

def load_cleaned_file(file_name,bucket_name,object_name=None):
    if object_name is None:
        object_name=os.path.basename(file_name)

    try:
        s3_client=boto3.client('s3')
        response=s3_client.upload_file(file_name,bucket_name,object_name)
        
    except ClientError as e:
        logging.error(e)
        return False

    return True