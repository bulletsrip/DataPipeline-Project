#!/bin/bash

#print service time
date

#virtualenv is now active
source /mnt/e/dataengineer/project/finalproject/venv/bin/activate

filetime=$(date +"%Y%m%d")

#running mapreduce on local

echo "[INFO] MapReduce is Running ....."
python3 /mnt/e/dataengineer/project/finalproject/mapreduce/MapReduce_Job.py /mnt/e/dataengineer/project/finalproject/local/transaction_$filetime.csv > /mnt/e/dataengineer/project/finalproject/data/processed_data/ProductSoldAnnually.csv
echo "[INFO] Mapreduce is Done ....."