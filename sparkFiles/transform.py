
# import packages - spark data manipulation functions
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import when, col, concat, concat_ws, coalesce, round

# import packages - reference code join
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# ~~~ CREATE SPARKSESSION ~~~ #
# Here, will have given the name to our Application by passing a string to .appName() as an argument. 
# Next, we used .getOrCreate() which will create and instantiate SparkSession into our object spark. 
# Using the .getOrCreate() method would use an existing SparkSession if one is already present else will create a new one.

spark = SparkSession.builder\
        .appName("PySpark DataFrame From External Files")\
        .getOrCreate()



# ~~~ CSV TO SPARK DATAFRAME ~~~ #

# pull source data output from previous task

file = '/opt/airflow/data/cleansedTransactions.csv'

# define a columns to import
columns = ['Transaction Date', 'Reference', 'Debit Amount','Credit Amount', 
           'Transaction Ref1', 'Transaction Ref2', 'Transaction Ref3']

# read csv into spark dataframe, specifying columns to import
# The * operator is used to unpack the list and pass the column names as arguments to the select method.
df = spark.read.csv(file, header=True, inferSchema=True).select(*columns)

# drop rows with missing value in all columns
df = df.na.drop(how='all')


# ~~~ TRANSFORMATION ~~~ #

# New column "TransactionType"
tTmp1 = df.withColumn("TransactionType", lit(""))

# Update 'Debit Amount' & 'Credit Amount' data type string to numerical. Round of to 2 decimal places
tTmp2 = tTmp1 \
        .withColumn("Debit Amount", round(tTmp1['Debit Amount'].cast('float'), 2)) \
        .withColumn("Credit Amount", round(tTmp1['Credit Amount'].cast('float'), 2))

# Chain transformations together and avoid creating unnecessary DataFrames
tTmp3 = tTmp2 \
        .withColumnRenamed("Transaction Date", "Date") \
        .withColumn("TransactionType", when(col("Credit Amount").isNotNull(), "IN").otherwise("OUT")) \
        .withColumn("Amount", coalesce(col("Debit Amount"), col("Credit Amount"))) \
        .withColumn("Sender", when(col("TransactionType") == "OUT", "My DBS Account") \
                                .otherwise(concat_ws(" ", col("Transaction Ref1"), col("Transaction Ref2"), col("Transaction Ref3")).cast("string"))) \
        .withColumn("Receiver", when(col("TransactionType") == "IN", "My DBS Account") \
                                .otherwise(concat_ws(" ", col("Transaction Ref1"), col("Transaction Ref2"), col("Transaction Ref3")).cast("string"))) \
        .withColumn("Receiver", regexp_replace("Receiver", " My DBS Account", "")) \
        .withColumn("Sender", regexp_replace("Sender", "NaN", "")) \
        .withColumn("Receiver", regexp_replace("Receiver", "NaN", ""))

# keep desired columns only
columns_to_keep = ['Date' , 'Reference', 'TransactionType', 'Amount', 'Sender', 'Receiver']
tTmp4 = tTmp3.select(*columns_to_keep)

# output transformed data to csv
output = tTmp4.toPandas()
output.to_csv(r'/opt/airflow/data/transformedTransactions.csv', index=False, mode='w')
