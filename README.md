# ETL Pipeline with Airflow, S3, Spark, and Postgres

---------

# About
Educational project on building an ETL (Extract, Transform, Load) data pipeline, orchestrated with Airflow.


## Overview

An AWS S3 bucket is used as a Data Lake in which weekly transaction history files(csv.) are stored. Bucket is scanned to pull the latest transaction file. File goes through pre-processing using Spark (PySpark) and loaded to target database(Postgres). 

Eventually a Personal Finance Dashboard will utilize the transaction data to provide valuable insight (WIP)

Entire process is orchestrated & scheduled using Apache Airflow.

Solution is containerized using Docker

Project is comprised of mainly 2 parts:

1. Main DAG file (orchestration config)
2. Pre-processing scripts (tasks to be executed)

The pipeline architecture - author's interpretation:
![data-architecture-v2 1](https://github.com/edzranhisham/End-To-End-ETL-For-Finance-Dashboard-VIZ/assets/91733415/591f7cf4-849e-4aff-965a-de4497edcc19)
Note: Since this project was built for learning purposes and as an example, it functions only for a single scenario and data schema.


## Dataset
Weekly transaction history data exported into CSV from DBS iBanking portal.

## Data Understanding
![data-understanding](https://github.com/edzranhisham/End-To-End-ETL-For-Finance-Dashboard-VIZ/assets/91733415/6d4330a3-a433-41e8-b577-8b969f1af5bc)

## Data Model
![data-model](https://github.com/edzranhisham/End-To-End-ETL-For-Finance-Dashboard-VIZ/assets/91733415/e35e36e8-1dd0-4e15-824b-8c35b3f00a81)

## Pre-requisites
- Docker
- Docker Compose
- AWS S3 Bucket
- Postgres Database

## ETL Steps/Tasks undertaken in Pipeline

https://github.com/edzranhisham/End-To-End-ETL-For-Finance-Dashboard-VIZ/assets/91733415/a36fbcbb-0833-469c-85f5-fac46aed140c

### Ingest
**extract_getLatestDate()**
- Establish connection to S3
- List files in s3 raw folder
- Get latest file name from list

**extract_readCsvData()**
- Use latest file name to read in CSV file from S3 into pandas dataframe for manipulation

### Cleanse
**Cleanse.py**
- Remove redundant/unwanted strings from "Transaction Reference" columns.
- Replace sensitive info such as account number to something generic (e.g. My Account)

### Transform
**Transform.py**
- Initiate spark session
- Convert to Spark dataframe
- Rename columns
- Derive new columns (e.g. Transaction Type, Sender, Receiver)

**Transform_join.py**
- Reference Codes join with Transaction Codes description

### Load
**Load.py**
- Connect to local Postgres database
- Append incremental dataset to transactions table in database

## Future improvements/to-do
- Dashboarding
- Unit testing
- Data Quality checks
- Alerting via slack/email

## References

[Setting up airflow](https://www.youtube.com/watch?v=aTaytcxy2Ck&pp=ygUSc2V0dGluZyB1cCBhaXJmbG93)

[Why incorporate docker in a project](https://medium.com/codex/airflow-and-spark-running-spark-jobs-on-airflow-docker-based-solution-fc6cc8794c9b)

[Connecting Docker to Locally Hosted Postgres Database](https://gist.github.com/MauricioMoraes/87d76577babd4e084cba70f63c04b07d)


## Base Concepts
- [Data Engineering](https://realpython.com/python-data-engineer/)
- [ETL (Extract, Transform, Load)](https://aws.amazon.com/what-is/etl/#:~:text=Extract%2C%20transform%2C%20and%20load%20(,and%20machine%20learning%20(ML).))
- [Pipeline](https://www.altexsoft.com/blog/datascience/what-is-data-engineering-explaining-data-pipeline-data-warehouse-and-data-engineer-role/)
- [Data Lake](https://aws.amazon.com/big-data/datalakes-and-analytics/what-is-a-data-lake/)
- [Data Warehouse](https://dataengineering.wiki/Concepts/Data+Warehouse)
- [Data Schema](https://towardsdatascience.com/designing-your-database-schema-best-practices-31843dc78a8d)
- [Apache Airflow](https://dataengineering.wiki/Tools/Workflow+Orchestrators/Apache+Airflow) - Workflow Orchestration Tool
- [Airflow DAG](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Airflow XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Airflow TaskAPI](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)
- [Apache Spark](https://spark.apache.org), specifically the [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) api
- [AWS S3](https://aws.amazon.com/s3/)
- [Postgres](https://www.postgresql.org)
- [Selenium](https://selenium-python.readthedocs.io)
- [Containerization](https://towardsdatascience.com/docker-101-all-you-wanted-to-know-about-docker-2dd0cb476f03) via [Docker](https://www.docker.com)


## Enabling Spark in local mode
Had difficulty finding the solution for this task - thus I have noted my steps to replicate

**Bolded** are the respective document/files involved

**requirements.txt**

Install required python packages
```
Pyspark
apache-airflow-providers-apache-spark
```

**docker-compose.yaml**

Context: I needed to find out where the PySpark package was being installed, in order to map the spark-home, spark-submit variables 

```
services:
  python-runner:
    image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.6.3}
    entrypoint: ["python", "-c", "import sys; print('\\n'.join(sys.path))"]
```

**Dockerfile**

Set path to Java installation and PySpark.

When initiating SparkSession, system will search for where PySpark is being installed via the path specified in the SPARK_HOME variable.

In the PySpark pacakge, it will search and execute 'spark-submit' script which is needed to run spark jobs.


```
# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
# Set the SPARK_HOME environment variable
ENV SPARK_HOME /home/airflow/.local/lib/python3.7/site-packages/pyspark
# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/home/airflow/.local/lib/python3.7/site-packages/pyspark
```

**Script itself**

Ensure SparkSession is being built in 'local mode'.

Normally, when setting configs, such as 'spark.executor.memory' a cluster may be needed.

To avoid this we do not need to specify any configurations. 

Below is how Spark can be executed in 'local mode':

```
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
```
