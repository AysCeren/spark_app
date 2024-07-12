# Spark Streaming Application
![image](https://github.com/AysCeren/spark_app/assets/154695340/3c5d3c9d-6a88-461a-bb3a-9b682e0ee4d7)

## What is Spark?
Apache Spark is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters. It is a data processing framework that can quickly perform processing tasks on very large data sets, and can also distribute data processing tasks across multiple computers, either on its own or in tandem with other distributed computing tools.

## What is Data Streaming?
Data streaming is the continuous transfer of data from one or more sources at a steady, high speed for processing into specific outputs. Combination of kafka and spark creates real-data streaming application.

## What is Stream Processing?
Stream processing is a core concept of “Big Data” processing/analysis and event-driven architecture. In it simplest form, stream processing is reading an incoming stream of data, or sequence of events, in real time and performing some actions on that data producing an output.

![image](https://github.com/user-attachments/assets/e9f382da-e0ed-4437-89f6-d656bb15f771)

## What does our project aim?

Integrating Kafka and Spark can help you build a reliable and scalable data processing pipeline that can handle real-time data streams and turn them actionable Complex Event Processes (CEPs). Therefore, this project enables kafka and spark integration to make stream processing.

### Prerequisites:
+ Kafka Producer (You can check kafka_producer app: )
+ Hadoop Installation
+ Spark Installation
> Note: Spark is built in Scala language, it may perform better in Scala that's PL is Scala.

## What is Hadoop and Why do we need Hadoop?
The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. However Spark and Hadoop seem similar, Spark is a enhanced model compared to Hadoop. It works simplier and faster.
Spark is using Hadoop's Distributed File System to store data instantly, so, hadoop installation is needed.
https://github.com/cdarlint/winutils You can use this link to access winutils.exe.
  + Hadoop Installation Steps:
    - Download winutils.exe
    - Create a file named Hadoop in C directory and open a new folder as 'bin'.
    - Move winutils.exe file, and set the environmental variables.
    - ![image](https://github.com/user-attachments/assets/6fdc5451-2e1f-40d1-b74a-a61d9e1f8b9e)
    - Also edit path variable as: %HADOOP_HOME%\bin
  + Spark Installation Steps:
    - Go to, https://spark.apache.org/downloads.html, download Spark
    - Extract downloaded files to C drive folder spark.
    - Set your environmental variables

## Builting Spark Application


 
