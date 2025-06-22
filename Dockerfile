FROM apache/airflow:3.0.1

USER root

# Install default JDK and clean up
RUN apt update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME manually (update path after install)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir pyspark

# COPY jars/*.jar /home/airflow/.local/lib/python3.12/site-packages/pyspark/jars/