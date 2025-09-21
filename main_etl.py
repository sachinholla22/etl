# main_etl.py

from etl.extract import download_from_s3, list_csv_files, find_csv_file
from etl.transform import transform_csv
import os

# Parameters
bucket_name = "my-bucket-2001-test"
s3_key = "hospital/patients.csv"
local_path = "../Data/patients.csv"
data_folder = "/Data"


# Step 1: Extract
downloaded = download_from_s3(bucket_name, s3_key, local_path)

# Step 2: Locate CSV file
if downloaded!=None:
    csv_files = list_csv_files(data_folder)
    patients_file = find_csv_file(csv_files, "patients.csv")
    if patients_file:
        print(f"Found file: {patients_file}")
        full_path=os.path.join(data_folder,patients_file)
        transformed_file=transform_csv(full_path)
    else:
        print("patients.csv not found in data folder.")
