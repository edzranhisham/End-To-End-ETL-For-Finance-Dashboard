# ~~~ START ~~~ #

import numpy as np
import pandas as pd
import os
from os import listdir
import fsspec
import inspect
import sys

import boto3 
import s3fs # s3 data transfer

# establish session with your aws account
session = boto3.Session( 
         aws_access_key_id='AKIA2JVX452SKNWQ6LZ6', 
         aws_secret_access_key='1TTh6KELTJ+CFDUIlC4ov4GNC5ItTGYSSAYgZ9+8',
         region_name='us-east-1')

# ~~~ LIST FILES IN RAW FOLDER ~~~ #
s3 = session.client('s3')

# empty list to store file names
s3fileList = []

def list_s3_contents(bucket_name, prefix):
        
        # list_objects_v2 returns dict response of metadata details for all files present in raw folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix) 
        
        # only extract values in Key value "Contents"
        # Value in the Key "Contents" is a list of nested dictionaries
        # iterate through the list, and for each dictionary, extract the value for the Key "Key"
        # the Key "Key" contains file name values 
        for i in range(len(response['Contents'])):
                if i > 0: # index 0 contains redundant value "raw/", thus start extracting from index 1
                    file = response['Contents'][i]['Key'].replace('raw/', '')
                    s3fileList.append(file)
        s3fileList

list_s3_contents('personal-finance-edz-ly','raw/')

# ~~~ GET LATEST FILE NAME ~~~ #

# extract the dates from filenames and convert string to integer
datesList = []
for csv in s3fileList:
    csv.split('_')
    datesList.append(csv.split('_')[1][:6])

# get the index of the latest date
maxDate = str(max(datesList))

# get the latest filename
latestFilename = 'transactions_' + maxDate + '.csv'

# ~~~ READING CSV ~~~ #
# Read a CSV file on S3 into a pandas data frame "df_latestFile" for manipulation

# s3 url to file in raw folder
s3url = os.path.join('s3://','personal-finance-edz-ly/','raw/',latestFilename)

aws_credentials = { "key": "AKIA2JVX452SKNWQ6LZ6", "secret": "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" }

df_latestFile = pd.read_csv(s3url, storage_options=aws_credentials, skiprows=17, index_col=False)

# drop missing rows with missing value in all columns
df_latestFile = df_latestFile.dropna(how='all')


# ~~~ LOAD PREPARED DATAFRAME INTO S3 BUCKET ~~~ #

# s3 url to file in prepared folder
s3url = os.path.join('s3://','personal-finance-edz-ly/','prepared/',latestFilename)

# load csv into s3 bucket
df_latestFile.to_csv(s3url,index=False,storage_options=aws_credentials)

# ~~~ END ~~~ #