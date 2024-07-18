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

  //Read The Stream
  val readStream = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","test")
    .option("startingOffsets","earliest")
    .load()

  readStream.printSchema()
  Console.println("Start to write your df to console:")
  val stringDF = readStream.selectExpr("CAST(value AS STRING)")
  //stringDF.writeStream.format("console").start() --> to check the stringDF

  //Note: The schema ensures the data types are enforced when DataFrame is ready.
  val dataSchema = new StructType()
    .add("id",IntegerType, nullable = false)
    .add("firstname", StringType, nullable = false)
    .add("lastname", StringType, nullable = false)
    .add("address", StringType, nullable = true)
    .add("phone", StringType, nullable = true)
    .add("loginDate", StringType, nullable = true)
    .add("birthDay", StringType, nullable = true)

  dataSchema.printTreeString()
  
  //Parsing for the contact from the string DF
  var contactDF = stringDF
    .select(explode(from_json(col("value"), ArrayType(dataSchema))).alias("data"))
    .select("data.*")

  //basic select stmt. as an  example
  contactDF.select("lastname").writeStream.format("console").start()

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
  //The columns will be removed if they are not in the phone format
   contactDF = contactDF
    .withColumn("phone", when(functions.length(col("phone")) =!= 13, defaultPhoneNumber).otherwise(col("phone"))

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
 

  //Why do we need this part: Because Kafka is recording data by grouping into keys and values, we are using key and value as columns
  val toWrite = contactDF.withColumn("key", col("firstname"))
    .withColumn("value", to_json(struct(col("firstname"), col("lastname"), col("address"), col("phone"), col("birthDay"), col("loginDate"))))

  val ds = toWrite
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "writeIntoTopic")
    .start()


}
