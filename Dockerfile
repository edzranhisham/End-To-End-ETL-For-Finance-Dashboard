FROM apache/airflow:2.6.3

USER root
# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Set the SPARK_HOME environment variable
ENV SPARK_HOME /home/airflow/.local/lib/python3.7/site-packages/pyspark

# Create SPARK_HOME env var
RUN export SPARK_HOME
ENV PATH $PATH:/home/airflow/.local/lib/python3.7/site-packages/pyspark

USER airflow
RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow
WORKDIR /opt/airflow
RUN pip install -r requirements.txt