import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace


# ~~~ CREATE SPARKSESSION ~~~ #
# Here, will have given the name to our Application by passing a string to .appName() as an argument. 
# Next, we used .getOrCreate() which will create and instantiate SparkSession into our object spark. 
# Using the .getOrCreate() method would use an existing SparkSession if one is already present else will create a new one.

spark = SparkSession.builder\
        .appName("PySpark DataFrame From External Files")\
        .getOrCreate()



# pull source data output from previous task

file = '/opt/airflow/data/rawTransactions.csv'

df = pd.read_csv(file, index_col=False, sep='\,')

df = df.dropna(how='all')

# df_latestFile.to_csv(r'/opt/airflow/data/testOutput.csv') # test whether script is being executed


# ~~~ DATAFRAME TO SPARK DATAFRAME ~~~ #

csv_file = spark.createDataFrame(df)


# ~~~ CLEANSING ~~~ #
# Consolidate all operations on the original DataFrame
# Avoid redundant Column Operations - Consolidate operations of 'regexp_replace' function to make code more efficient
# Minimize dataframe materialization - Avoid creating too many intermediate 'temp' dataframes to reduce memory usage
cTmp1 = csv_file \
    .replace("5264-7110-3357-3710", "My DBS Account") \
    .withColumn("Transaction Ref1", regexp_replace("Transaction Ref1", " SI NG.*|From: |To: |TO: ", "")) \
    .withColumn("Transaction Ref2", regexp_replace("Transaction Ref2", " SI NG.*|From: |To: |TO: ", ""))

output = cTmp1.toPandas()

# store cleansed data into intemediary storage as input for next task
output.to_csv(r'/opt/airflow/data/cleansedTransactions.csv')