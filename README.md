# ETL Pipeline with Selenium, Airflow, S3, Spark, and Postgres

---------

# About

- Brief description on project
Educational project on how to build an ETL (Extract, Transform, Load) data pipeline, orchestrated with Airflow.
- <gif of airflow etl pipeline running>

An AWS s3 bucket is used as a Data Lake in which CSV transaction files are stored. The data is extracted via web automation (Selenium) and parsed (cleaned). It is then transformed/processed with Spark (PySpark) and loaded/stored in a Postgres database.

The pipeline architecture - author's interpretation:

- Overview Diagram of pipeline architecture
<img width="1339" alt="Data-Architecture" src="https://github.com/edzranhisham/End-To-End-ETL-For-Finance-Dashboard-VIZ/assets/91733415/5f5ea3b9-196f-4446-9db7-1091bfed0ee5">
Note: Since this project was built for learning purposes and as an example, it functions only for a single scenario and data schema.

# Scenario
- Track finances on day-to-day level without the use of 3rd party finance applications 

# Base Concepts
- [Data Engineering]([url](https://realpython.com/python-data-engineer/))
- [ETL (Extract, Transform, Load)]([url](https://aws.amazon.com/what-is/etl/#:~:text=Extract%2C%20transform%2C%20and%20load%20(,and%20machine%20learning%20(ML).))
- [Pipeline]([url](https://www.altexsoft.com/blog/datascience/what-is-data-engineering-explaining-data-pipeline-data-warehouse-and-data-engineer-role/))
- [Data Lake]([url](https://aws.amazon.com/big-data/datalakes-and-analytics/what-is-a-data-lake/))
- [Data Warehouse]([url](https://dataengineering.wiki/Concepts/Data+Warehouse))
- [Data Schema]([url](https://towardsdatascience.com/designing-your-database-schema-best-practices-31843dc78a8d))
- [Apache Airflow]([url](https://dataengineering.wiki/Tools/Workflow+Orchestrators/Apache+Airflow)) - Workflow Orchestration Tool
- [Airflow DAG]([url](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html))
- [Airflow XCom]([url](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html))
- [Airflow TaskAPI]([url](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html))
- [Apache Spark]([url](https://spark.apache.org)), specifically the [PySpark]([url](https://spark.apache.org/docs/latest/api/python/index.html)) api
- [AWS S3]([url](https://aws.amazon.com/s3/))
- [Postgres]([url](https://www.postgresql.org))
- [Selenium]([url](https://selenium-python.readthedocs.io))
- [Containerization]([url](https://towardsdatascience.com/docker-101-all-you-wanted-to-know-about-docker-2dd0cb476f03)) via [Docker]([url](https://www.docker.com))
- Networking in Docker

# Pre-requisites
- Docker
- Docker Compose
- AWS S3 Bucket
- Postgres Database

# Setup
- S3 bucket
- Get airflow docker-compose .yaml file [here]([url](https://airflow.apache.org/docs/apache-airflow/2.7.1/docker-compose.yaml))
- Create respective folders
- Create airflow env variables
- Init airflow instance
- Check container status via docker ps
- Test airflow via localhost:8080

# Enabling AWS Connection
- follow steps to save credentials in airflow ui to enable aws connection for S3 hook

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
