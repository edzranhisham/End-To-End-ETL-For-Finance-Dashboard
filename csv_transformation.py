# ~~~ START ~~~ #

import numpy as np
import pandas as pd
import os
from os import listdir
import inspect
import sys


# link to source folder directory
folderPath = "/Users/mac/Desktop/VSCode/Projects/Development-Personal-Finance/src_transactions/"


# ~~~ GET FILENAMES ~~~ #

# get list of filenames in directory
def find_csv_filenames(path_to_dir, suffix='.csv'):
    filenames = listdir(path_to_dir)
    return[filename for filename in filenames if filename.endswith(suffix)]

filenames = find_csv_filenames(folderPath, suffix=".csv")

for csv in filenames:
    print(csv)


# ~~~ GET LATEST FILE NAME ~~~ #

# extract the dates from filenames and convert string to integer
dates = [int(csv.split('_')[1][:6]) for csv in filenames]

# get the index of the latest date
maxDate = str(max(dates))

# get the latest filename
latestFile = 'transactions_' + maxDate + '.csv'



# ~~~ READING CSV ~~~ #

# read latest file starting from row 7
df = pd.read_csv(folderPath + latestFile, skiprows=6, index_col=False)

# drop missing rows with missing value in all columns
df = df.dropna(how='all')

# preview final dataframe
df.head()

print(df.columns)


# ~~~ SAVE PREPARED CSV ~~~ #

# get current working directory
script_path = os.path.abspath(os.getcwd())

# appened desired folder path to store prepared transactions
filePath = os.path.join(script_path, "prepared_transactions")

# appened desired file name to folder path
# e.g. ../prepared_transactions/transactions_1900523.csv
fileNamePath = os.path.join(filePath, latestFile)

# create "prepared_transactions" folder if not exist
if not os.path.exists(filePath):
    os.makedirs(filePath)

# convert to csv and load to desired local folder
df.to_csv(fileNamePath, index=False)


# ~~~ LOAD PREPARED CSV INTO S3 BUCKET ~~~ #

# connect to s3 bucket 

# load csv into s3 bucket

import boto3

# establish session with your aws account
session = boto3.Session( 
         aws_access_key_id='AKIA2JVX452SKNWQ6LZ6', 
         aws_secret_access_key='1TTh6KELTJ+CFDUIlC4ov4GNC5ItTGYSSAYgZ9+8',
         region_name='us-east-1')

# function to upload prepared csv to s3 bucket
def upload_csv_to_s3(file_path, bucket_name, key):
    s3 = session.client('s3')
    s3.upload_file(file_path, bucket_name, key)

# Specify the name of your S3 bucket and Key
bucket_name = 'personal-finance-edz-ly'

key = 'prepared/' + latestFile # e.g. raw/transactions_190523.csv

# Specify the local file path containing the prepared csv
file_path = fileNamePath

# Upload the CSV file to the specified S3 bucket and folder
upload_csv_to_s3(file_path, bucket_name, key)

# ~~~ END ~~~ #