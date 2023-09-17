
# ~~~ LIBRARIES ~~~ #
import psycopg2
from pyspark.sql import SparkSession
import pandas as pd
import sys

# ~~~ FUNCTIONS ~~~ #
def connect(host, dbname, user, password):

   '''
   Connect to database
   '''
   conn = None
   try:
      print('Connecting…')
      conn = psycopg2.connect(
                   host = host,
                   database = dbname,
                   user = user,
                   password = password)
   except (Exception, psycopg2.DatabaseError) as error:
       print(error)
       sys.exit(1)
   print('All good, Connection successful!')
   return conn


def sql_to_dataframe(conn, query, column_names):
   '''
   Import data from a PostgreSQL database using a SELECT query 
   ''' 
   # start a cursor to perform database operations
   cursor = conn.cursor() 
   try:

      df = pd.DataFrame() # initialize empty dataframe, else will throw reference error from return statement

      cursor.execute(query)
      # The execute returns a list of tuples:
      tuples_list = cursor.fetchall()
      # Transform the list into a pandas DataFrame:
      df = pd.DataFrame(tuples_list, columns=column_names)
   
   except (Exception, psycopg2.DatabaseError) as error:
      print("Error: %s” % error")
   
   finally: # placing cursor.close() in a 'finally' gurantees that cursor is closed whether or not an exception occurs
      cursor.close()
   
   return df

# ~~~ CREATE SPARK SESSION ~~~ #
# Here, will have given the name to our Application by passing a string to .appName() as an argument. 
# Next, we used .getOrCreate() which will create and instantiate SparkSession into our object spark. 
# Using the .getOrCreate() method would use an existing SparkSession if one is already present else will create a new one.
spark = SparkSession.builder\
        .appName("PySpark DataFrame From External Files")\
        .getOrCreate()

# ~~~ IMPORT REFERENCE CODES FROM DATABASE ~~~ #

# Database connection parameters
host = "192.168.1.15"  # Use the host's IP address
port = 5432  # Default PostgreSQL port
dbname = "personal-finance"
user = "postgres"
password = ""

# opening the connection
conn = connect(host=host, dbname=dbname, user=user, password=password)

query = '''
SELECT "Code", "Description"
FROM fond.dbstransactioncodes
'''

column_names = ['Code', 'Description']

# loading reference codes as dataframe
src_refCodes = sql_to_dataframe(conn, query, column_names)
conn.close()

# convert reference codes from pandas to spark dataframe
refCodes = spark.createDataFrame(src_refCodes)

# ~~~ IMPORT TRANSACTIONS ~~~ #
# pull source data output from previous task
file = '/opt/airflow/data/transformedTransactions.csv'

# define a columns to import
columns_to_import = ['Date' , 'Reference', 'TransactionType', 'Amount', 'Sender', 'Receiver']

# read csv into spark dataframe, specifying columns to import
# The * operator is used to unpack the list and pass the column names as arguments to the select method.
transactions = spark.read.csv(file, header=True, inferSchema=True).select(*columns_to_import)


# ~~~ REFERENCE CODES JOIN WITH TRANSACTIONS ~~~ #

# register dataframe as temp tables
transactions.createOrReplaceTempView('tmpTransactions')
refCodes.createOrReplaceTempView('tmpReference')

# sql query to update transactions table with reference codes
updatedTransactions = spark.sql("""
                 SELECT Date,  TransactionType, Description, Amount, Sender, Receiver 
                 from tmpTransactions as tbl1 
                 inner join tmpReference as tbl2 
                 on tbl1.Reference = tbl2.Code""")

output = updatedTransactions.toPandas()

output.to_csv(r'/opt/airflow/data/transformJoinTransactions.csv', index=False, mode='w')


# set up the connection to the Postgres database
#conn = psycopg2.connect(f'dbname={dbname} host={host} port={port} user={user} password={password}')

# start a cursor to perform database operations
#cursor = conn.cursor()




