import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._ //{col, current_date, date_format, datediff, explode, from_json, lit, struct, to_date, to_json, unix_timestamp, when}
import org.apache.spark.sql.types._

object NewSparkStreaming extends App{


  val conf = new SparkConf().setAppName("SparkKafkaStreaming")
    .set("spark.streaming.stopGracefullyOnShutdown","true")
    .set("spark.hadoop.hadoop.home.dir", "C:\\hadoop") // Set Hadoop home directory
    .set("spark.driver.extraLibraryPath", "C:\\hadoop\\bin") // Set path to directory containing winutils.exe
    .set("spark.executor.extraLibraryPath", "C:\\hadoop\\bin") // Set path to directory containing winutils.exe for executors

  val spark = SparkSession.builder()
    .master("local[2]") //where the spark will run
    .config(conf)  // Use local file system
    .getOrCreate()

  val topicName = "test"
  //Read The Stream
  val readStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe",topicName)
    .option("startingOffsets","earliest")
    .load()

  readStream.printSchema()

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
  
  //dataSchema.printTreeString() --> You can check the schema's structure

  var contactDF = stringDF
    .select(explode(from_json(col("value"), ArrayType(dataSchema))).alias("data"))
    .select("data.*")

  /*Different Spark Jobs, Transformations*/
  
  //This is cleaning the data if there is a wrong-typed data, or if the column name is wanted to be changed
  contactDF.select(
    col("firstname").cast(StringType).as("firstname"),
    col("lastname").cast(StringType).as("lastname"),
    col("address").cast(StringType).as("address"),
    col("phone").cast(StringType).as("phone"),
    col("birthDay").cast(StringType).as("birthDay"),
    col("loginDate").cast(StringType).as("loginDate")
  )

  //Filling the null values for the DataFrame:
  contactDF = contactDF.na.fill("-", Seq("firstname"))
    .na.fill("-",Seq("lastname"))
    .na.fill("-", Seq("address"))
    .na.fill("-",Seq("phone"))
    .na.fill("-", Seq("birthDay"))
    .na.fill("-", Seq("loginDate"))

  private val defaultPhoneNumber = "+901111111111"
  //only the columns having more than 13 char.
   contactDF = contactDF
    .withColumn("phone", when(functions.length(col("phone")) =!= 13, defaultPhoneNumber).otherwise(col("phone")))

  //Converting String to Date, coming string format: dd-MM-yyyy, resulted Date Format: YYYY - MM - dd
  contactDF =  contactDF.withColumn("loginDate",to_date(unix_timestamp(contactDF.col("loginDate"), "dd-MM-yyyy").cast("timestamp")))
  contactDF =  contactDF.withColumn("birthDay",to_date(unix_timestamp(contactDF.col("birthDay"), "dd-MM-yyyy").cast("timestamp")))
  contactDF = contactDF.withColumn("age", (datediff(current_date(), col("birthDay"))/365).cast(IntegerType))


   val query =  contactDF
    .withColumn("key", contactDF.col("firstname"))
    .withColumn("value", to_json(struct(contactDF.col("firstname"), col("lastname"), col("address"), col("phone"), col("birthDay"), contactDF.col("age"),col("loginDate")))).writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "writeIntoTopic").option("checkpointLocation", "/tmp/vaquarkhan/checkpoint")// <-- checkpoint directory
    .start()

  // Write to console.
  contactDF.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate","true")
    .start()
  
  query.awaitTermination() //Helps to program to work continiously
}
