# ETL Pipeline with Selenium, Airflow, S3, Spark, and Postgres

---------

# About

- Brief description on project
- Overview Diagram of pipeline architecture


# Scenario
- daily bank transactions
- store into postgres for visualization

# Base Concepts
- Data Engineering
- ETL (Extract, Transform, Load)
- Pipeline
- Data Lake
- Data Warehouse
- Data Schema
- Apache Airflow
- Airflow DAG
- Airflow XCom
- Apache Spark, specifically the Pyspark api
- s3
- Postgres
- Selenium

# Pre-requisites
- Docker
- Docker Compose
- AWS s3 Bucket
- Postgres Database

# Setup
- S3 bucket
- Get docker-compose .yaml file from <link found in onenote>
- Create respective folders
- Create airflow env variables
- Init airflow instance
- Check container status via docker ps
- Test airflow via localhost:8080

# Running Spark jobs
- follow steps noted down in onenote

# Connecting Docker to Locally Hosted Postgres Database
- follow steps noted down in onenote

# ETL Steps/Tasks undertaken in Pipeline
- extract
- cleanse
- transform
- transform_join
- load

# Best Practices
- DAG as a config file
- functional programming

# Future improvements
- Unit testing
- Data Quality checks
- Alerting via slack/email

# References
