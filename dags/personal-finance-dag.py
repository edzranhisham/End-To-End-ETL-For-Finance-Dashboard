# LIBRARIES
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook # aws s3 data transfer

import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta

# aws libs
import boto3 
import s3fs # s3 data transfer
import fsspec

# DAG - plan DAG run/pipeline

# set up DAG arguments

AWS_S3_CONN_ID = "S3_default"

defaultArgs = {
      'owner': 'Edzran_Hisham',
      'start_date': datetime(2021,1,1),
      'retries': 2,
      'retries_delay': timedelta(seconds=30)
}

@dag(
      dag_id = "personal-finance-dag",
      schedule_interval = '@daily',
      default_args = defaultArgs,
      catchup=False
)

# DEFINE PIPELINE

def pipeline(): 

        hook = S3Hook(AWS_S3_CONN_ID)

        # TASKS/FUNCTIONS

        @task()
        def extract_getLatestDate():
                """
                Function Summary:
                
                1. establish connection to aws s3
                2. list files in s3 raw folder
                3. get latest file name from list

                """

                # connect to s3 via a S3Hook
                # establish session with your aws account

                
                
                # ~~~ START: list files in s3 raw folder ~~~ #
                
                response = hook.list_keys(bucket_name='bucket-name', prefix='raw/')
                # results: ['raw/', 'raw/transactions_010623.csv', 'raw/transactions_030623.csv', 'raw/transactions_140623.csv', 'raw/transactions_310823.csv']

                
                # empty list to store file names
                s3fileList = []
                
                # only extract values in Key value "Contents"
                # Value in the Key "Contents" is a list of nested dictionaries
                # iterate through the list, and for each dictionary, extract the value for the Key "Key"
                # the Key "Key" contains file name values 
                for i in range(len(response)):
                        if i > 0: # index 0 contains redundant value "raw/", thus start extracting from index 1
                                file = response[i].replace('raw/', '')
                                s3fileList.append(file)
        

                # ~~~ END: list files in s3 raw folder ~~~ #
                
                
                
                # ~~~ START: get latest filename ~~~ #

                # extract the dates from filenames and convert string to integer
                dateNumList = []
                for csv in s3fileList:
                        csv.split('_')
                        dateNumList.append(csv.split('_')[1][:6])

                # convert each date string to integer, then to date type
                dateList = []
                for dateNum in dateNumList:
                        day = int(dateNum[:2])
                        month = int(dateNum[2:4])
                        year = int(dateNum[4:]) + 2000
                
                date = datetime(year, month, day).date()
                dateList.append(date)

                # get the max date type
                latestDate = max(dateList)

                formatted_date = latestDate.strftime('%d%m%y')

                # get the latest filename
                # latestFilename = 'transactions_' + formatted_date + '.csv'

                latestFilename = f'raw/transactions_{formatted_date}.csv'

                return latestFilename

                # ~~~ END: get latest filename ~~~ #
        
        
        @task()
        def extract_readCsvData(latestFilename):
                """
                Function Summary: read a CSV file on S3 into a pandas data frame "df_latestFile" for manipulation

                """

                # s3 url to file in raw folder
                
                file = hook.download_file(key = latestFilename, 
                                          bucket_name = 'bucket-name')

                # start reading from row 18 onwards
                df_latestFile = pd.read_csv(file, skiprows=17, index_col=False, sep='\,')

                # drop missing rows with missing value in all columns 
                df_latestFile = df_latestFile.dropna(how='all')

                # store prepared raw data into intemediary storage 
                df_latestFile.to_csv(r'/opt/airflow/data/rawTransactions.csv')


        
        cleanse = BashOperator(
                task_id = 'cleanse',
                bash_command= 'python /opt/airflow/sparkFiles/cleanse.py'
        )

        transform = BashOperator(
                task_id = 'transform',
                bash_command= 'python /opt/airflow/sparkFiles/transform.py'
        )

        transform_join = BashOperator(
                task_id = 'transform_join',
                bash_command= 'python /opt/airflow/sparkFiles/transform_join.py'
        )

        load = BashOperator(
                task_id = 'load',
                bash_command = 'python /opt/airflow/sparkFiles/load.py'
        )

        
        
        # DEPENDENCY FLOW
        
        get_latest_date = extract_getLatestDate()
        read_csv_data = extract_readCsvData(get_latest_date) >> cleanse >> transform >> transform_join >> load
        
        
pipeline()