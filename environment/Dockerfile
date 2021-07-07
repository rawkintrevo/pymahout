FROM python:3.7

ADD spark-2.4.7-bin-hadoop2.7.tgz /tmp/spark-2.4.7-bin-hadoop2.7.tgz

RUN apt-get update && apt-get install default-jdk -y
RUN pip install pyspark==2.4.5

RUN mv /tmp/spark-2.4.7-bin-hadoop2.7.tgz/spark-2.4.7-bin-hadoop2.7 /opt/spark

ADD apache-mahout-14.2-SNAPSHOT-dependency-reduced.jar /opt/spark/jars
ADD mahout-core-14.2-SNAPSHOT-scala_2.11.jar /opt/spark/jars
ADD mahout-spark-14.2-SNAPSHOT-scala_2.11.jar /opt/spark/jars
ADD mahout-hdfs-14.2-SNAPSHOT.jar /opt/spark/jars

ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
