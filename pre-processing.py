import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import when, col, concat, concat_ws, coalesce
from datetime import datetime

# CREATE SPARKSESSION
# Here, will have given the name to our Application by passing a string to .appName() as an argument. 
# Next, we used .getOrCreate() which will create and instantiate SparkSession into our object spark. 
# Using the .getOrCreate() method would use an existing SparkSession if one is already present else will create a new one.

spark = SparkSession.builder\
        .master("local[*]")\
        .appName("PySpark DataFrame From External Files")\
        .getOrCreate()

# IMPORT OF CSV FROM S3

import os
import boto3
import s3fs

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
        
        # list_objects_v2 returns dict response of metadata details for all files present in prepared folder
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix) 
        
        # only extract values in Key value "Contents"
        # Value in the Key "Contents" is a list of nested dictionaries
        # iterate through the list, and for each dictionary, extract the value for the Key "Key"
        # the Key "Key" contains file name values 
        for i in range(len(response['Contents'])):
                if i > 0: # index 0 contains redundant value "prepared/", thus start extracting from index 1
                    file = response['Contents'][i]['Key'].replace('prepared/', '')
                    s3fileList.append(file)
        s3fileList

list_s3_contents('personal-finance-edz-ly','prepared/')

# ~~~ GET LATEST FILE NAME ~~~ #

# extract the dates from filenames. store in dateNumList
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
latestFilename = 'transactions_' + formatted_date + '.csv'


# ~~~ READING CSV ~~~ #
# Read a CSV file on S3 into a pandas data frame "df_latestFile" for manipulation

# s3 url to file in raw folder
s3url = os.path.join('s3://','personal-finance-edz-ly/','prepared/',latestFilename)

aws_credentials = { "key": "AKIA2JVX452SKNWQ6LZ6", "secret": "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" }

transactions = pd.read_csv(s3url, storage_options=aws_credentials, header=0)


# convert to dataframe to spark dataframe

csv_file = spark.createDataFrame(transactions)

# CLEANSING
#1 Replace "<Personal Acct Number>" with Savings
cTmp1 = csv_file.replace("5264-7110-3357-3710", "My DBS Account")

#2 Keep vendor names only
cTmp2 = cTmp1.withColumn("Transaction Ref1", regexp_replace("Transaction Ref1", " SI NG.*", ""))
cTmp3 = cTmp2.withColumn("Transaction Ref2", regexp_replace("Transaction Ref2", " SI NG.*", ""))

#3 Keep recipient names when starting with "From:" and "To:"

# "From: "
cTmp4 = cTmp3.withColumn("Transaction Ref1", regexp_replace("Transaction Ref1", "From: ", ""))
cTmp4 = cTmp3.withColumn("Transaction Ref2", regexp_replace("Transaction Ref2", "From: ", ""))

# "To: "
cTmp5 = cTmp4.withColumn("Transaction Ref1", regexp_replace("Transaction Ref1", "To: ", ""))
cTmp6 = cTmp5.withColumn("Transaction Ref2", regexp_replace("Transaction Ref2", "To: ", ""))

# "TO: "
cTmp7 = cTmp5.withColumn("Transaction Ref1", regexp_replace("Transaction Ref1", "TO: ", ""))
cTmp8 = cTmp6.withColumn("Transaction Ref2", regexp_replace("Transaction Ref2", "TO: ", ""))

# TRANSFORMATION

# 1) Rename "Transaction Date" -> "Date"

tTmp1 = cTmp8.withColumnRenamed("Transaction Date", "Date")

# 2) New column "TransactionType"
tTmp2 = tTmp1.withColumn("TransactionType", lit(""))

# 3) "TransactionType" logic

# Update 'Debit Amount' & 'Credit Amount' data type string to numerical 

tTmp3 = tTmp2.withColumn("Debit Amount", tTmp2['Debit Amount'].cast('float'))
tTmp4 = tTmp3.withColumn("Credit Amount", tTmp3['Credit Amount'].cast('float'))

# Update 'TransactionType' column
# When 'Credit Amount' is not null = "IN"
# When 'Debit Amount' is not null = "OUT"
tTmp5 = tTmp4.withColumn("TransactionType", when(col('Credit Amount').isNotNull(), 'IN').otherwise('OUT'))

# 4) New column "Amount"

tTmp6 = tTmp5.withColumn("Amount", coalesce(tTmp5['Debit Amount'], tTmp5['Credit Amount']))

# 5) New column "Sender" and "Receiver" for Sender and Receiver scenarios

# Set 'Sender' to be 'My DBS Account' for outgoing transactions
tTmp7 = tTmp6.withColumn("Sender", when(col('TransactionType') == "OUT", 'My DBS Account'))

# When Credit Amount is not null, set as incoming transaction, else it is outgoing transaction
tTmp5 = tTmp4.withColumn("TransactionType", when(col('Credit Amount').isNotNull(), 'IN').otherwise('OUT'))

# If TransactionType = 'OUT', Receiver = concat transaction descriptions columns together
# If TransactionType = 'IN' = Receiver = 'My DBS Account'
tTmp8 = tTmp7.withColumn('Receiver', when(col('TransactionType') == 'OUT', 
                                          concat_ws(" ",tTmp7['Transaction Ref1'], tTmp7['Transaction Ref2'], tTmp7['Transaction Ref3'])).otherwise('My DBS Account'))

# leave only vendor/acct names by removing unwanted strings(e.g. ' My DBS Account')
tTmp9 = tTmp8.withColumn("Receiver", regexp_replace("Receiver", " My DBS Account", ""))

tTmp9.select(tTmp9['Reference'],tTmp9['TransactionType'],tTmp9['Transaction Ref1'], tTmp9['Transaction Ref2'], tTmp9['Transaction Ref3'], 
             tTmp9['Sender'], tTmp9['Receiver']).show(n=50,truncate=False)

# If TransactionType = 'OUT', Receiver = concat transaction descriptions columns together
# If TransactionType = 'IN' = Receiver = 'My DBS Account'
tTmp10 = tTmp9.withColumn("Sender", when(col('TransactionType') == 'IN',
                          concat_ws(" ",tTmp9['Transaction Ref1'], tTmp9['Transaction Ref2'], tTmp9['Transaction Ref3'])).otherwise(tTmp9['Sender']))

tTmp10.select(tTmp10['Reference'],tTmp10['TransactionType'],tTmp10['Transaction Ref1'], tTmp10['Transaction Ref2'], tTmp10['Transaction Ref3'], 
             tTmp10['Sender'], tTmp10['Receiver']).show(n=50,truncate=False)

# drop unused columns 'Debit Amount', 'Credit Amount', 'Transaction Ref1', 'Transaction Ref2', 'Transaction Ref3'

tTmp11 = tTmp10.drop('Debit Amount', 'Credit Amount', 'Transaction Ref1', 'Transaction Ref2', 'Transaction Ref3')

# Reference Codes Join

# Read reference codes excel file
sourceFile = "/Users/mac/Desktop/VSCode/Projects/Development-Personal-Finance/test_data/DBS Transaction Codes.xlsx"

# using pandas to read in .xlsx file
src_refCodes = pd.read_excel(sourceFile,header=0)

# iterating through rows
# split codes based on " / " delimitter

new_data = []

for index, row in src_refCodes.iterrows():
    if " / " in row["Code"]:
        code_parts = row["Code"].split(' / ')
        for code_part in code_parts:
            new_data.append((code_part, row["Description"]))

    else:
        new_data.append((row["Code"], row["Description"]))

# convert list to dataframe

refCodesFinal = pd.DataFrame(new_data, columns=['Code','Description'])

# convert dataframe to pyspark dataframe

refCodes = spark.createDataFrame(refCodesFinal)

# register dataframe as a temporary table

tTmp11.createOrReplaceTempView('tmpTransactions')
refCodes.createOrReplaceTempView('tmpReference')

# sql query to update transactions table with reference codes

updatedTransactions = spark.sql("""
                 select Date, TransactionType, Description, Amount, Sender, Receiver 
                 from tmpTransactions as tbl1 
                 inner join tmpReference as tbl2 
                 on tbl1.Reference = tbl2.Code""")

updatedTransactions.show(n=50, truncate=False)

### To continue:
# load to database

