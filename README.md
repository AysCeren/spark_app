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
+ Kafka Producer (You can check kafka_producer app: https://github.com/AysCeren/kafka_producer)
+ Hadoop Installation
+ Spark Installation
> Note: Spark is built in Scala language, it may perform better in Scala that's PL is Scala. However, it can be written in Java, Python (PysSpark), R etc.

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

 ### Connection to the SparkSession
```
  val conf = new SparkConf().setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyOnShutdown","true")
    .set("spark.hadoop.hadoop.home.dir", "C:\\hadoop") // Set Hadoop home directory
    .set("spark.driver.extraLibraryPath", "C:\\hadoop\\bin") // Set path to directory containing winutils.exe
    .set("spark.executor.extraLibraryPath", "C:\\hadoop\\bin") // Set path to directory containing winutils.exe for executors

  val spark = SparkSession.builder()
    .master("local[2]") //where the spark will run
    .config(conf)  // Use local file system
    .getOrCreate()
```
+ 'conf' variable carries the setups,as spark executor, hadoop direction, library path and spark streaming.
+ **Meaning of stopGracefullyOnShutdown:**  Spark Streaming jobs are long-running jobs and their tasks need to be executed 7*24 hours, but there are cases like upgrading the code or updating the Spark configuration, where the program needs to be actively stopped. However, for distributed programs, there is no way to kill processes one by one. It is very important to shut down all configurations gracefully.
+ After conf variable is ready, SparkSession builder is started with these configuration features. (.config(conf))
+ .master[local[*]) says the program will run on local machine, * number of threads. Threads: are the virtual components or codes, which divide the physical core of a CPU into multiple virtual cores.
  
 ### Read from Kafka

 ```
val topicName = "test"
  //Read The Stream
  val readStream = spark.readStream
    .format("kafka") //determines the format
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe",topicName)
    .option("startingOffsets","earliest")
    .load()
```
+ The lines of codes are creating streaming DataFrames that are different than default DataFrames. The data came to these frames from Kafka.
+ kafka servers are running localhost, port 9092 as default. You can check by examining server.properties.
    - Start kafka servers firstly not to have exception: (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
+ topicName points which topic will be used to read operation.
  #### Structure of readStream
  ![image](https://github.com/user-attachments/assets/30077215-c200-4906-b109-ef0332beedcb)
  + After readStream is created, it has following attributes. The value column in the DataFrame created by spark.readStream from Kafka contains the payload of the Kafka messages. This payload is in _binary_ format (binary type). The actual content and structure of the value depend on what you have produced to the Kafka topic. It could be any serialized data, such as: Plain text, JSON, Avro, Protobuf, Any other serialized format
To work with the value data, you often need to deserialize it into a more structured format.

## Serializing value attribute and creating schema

```
val stringDF = readStream.selectExpr("CAST(value AS STRING)")

//Note: The schema ensures the data types are enforced when DataFrame is ready.
  val dataSchema = new StructType()
    .add("id",IntegerType, nullable = false)
    .add("firstname", StringType, nullable = false)
    .add("lastname", StringType, nullable = false)
    .add("address", StringType, nullable = true)
    .add("phone", StringType, nullable = true)
    .add("loginDate", StringType, nullable = true)
    .add("birthDay", StringType, nullable = true)
 
  var contactDF = stringDF
    .select(explode(from_json(col("value"), ArrayType(dataSchema))).alias("data"))
    .select("data.*")
```
+ The first step of deserialization: stringDF created. Converting the raw binary data into a human-readable string format. 
+ dataSchema represents the data that will come from kafka, it has the attributes of the Contact object (check the Kafka Producer example: )
+ The second step of deserialization: the last line of code tells that stringDF value String, will be formatted from 'JSON' to schema proper dataFrame.**

## Various Spark Jobs?
###### The program aims to tranform the data, so there are many jobs to process data. 

  - 1. Validate data, this job organize columns with correct data types and attribute names
  ```
 //This is cleaning the data if there is a wrong-typed data, or if the column name is wanted to be changed
  contactDF.select(
    col("firstname").cast(StringType).as("firstname"),
    col("lastname").cast(StringType).as("lastname"),
    col("address").cast(StringType).as("address"),
    col("phone").cast(StringType).as("phone"),
    col("birthDay").cast(StringType).as("birthDay"),
    col("loginDate").cast(StringType).as("loginDate")
  )
  ```
  - 2. Dropping null values
```
//Filling the null values for the DataFrame:
  contactDF = contactDF.na.fill("-", Seq("firstname"))
    .na.fill("-",Seq("lastname"))
    .na.fill("-", Seq("address"))
    .na.fill("-",Seq("phone"))
    .na.fill("-", Seq("birthDay"))
    .na.fill("-", Seq("loginDate"))
```
**Note: Different process can be implemented, check the following code for the next steps.**

## writeStream method
We can write DF to console or another kafka topic. You can two examples in this project. 


```
 // Write to console.
  contactDF.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate","true")
    .start()
  query.awaitTermination()
```
> It is important to add awaitTermination() method, it forces the application to listen write into kafka toics continously without shutting down.

## Important Reminders:

![image](https://github.com/user-attachments/assets/bcf52e81-dfd1-4ae8-bb6b-fc11280be095)

+ IntelliJ is used as IDE. It is important to choose language as Scala
+ Please control the version of spark and scala.
+ Because Scala runs on the Java platform (Java virtual machine), it is important to choose JDK. And working with Java 11 is recommended.**
