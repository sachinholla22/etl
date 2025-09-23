# main_etl.py

from extract import download_from_s3, list_csv_files, find_csv_file
from transform import transform_csv
from load import load_cleaned_file
import os

# Parameters
bucket_name = "my-bucket-2001-test"
s3_key = "hospital/patients.csv"
local_path = "Data/patients.csv"
data_folder = "/Data"



# Step 1: Extract
downloaded = download_from_s3(bucket_name, s3_key, local_path)

cleaned_file=transform_csv(downloaded)
print(cleaned_file)
res=load_cleaned_file(cleaned_file,bucket_name)
print(res)


