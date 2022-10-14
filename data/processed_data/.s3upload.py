import os
import glob
import boto3

BUCKET_NAME = 'de8-datalake'
FOLDER_NAME = 'processed_data'

s3 = boto3.client(
    's3', 
    region_name='us-east-1', 
    aws_access_key_id='secret', 
    aws_secret_access_key='secret'
    )

csv_files = glob.glob("/mnt/e/dataengineer/project/finalproject/data/processed_data/*.csv")
for filename in csv_files:
    key = "%s/%s" % (FOLDER_NAME, os.path.basename(filename))
    print("Putting %s as %s" % (filename,key))
    s3.upload_file(filename, BUCKET_NAME, key)
