FROM apache/airflow:2.8.1

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean

ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN apt-get install -y wget && wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /opt/ && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

ENV SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV PATH=$PATH:$SPARK_HOME/bin
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark fpdf