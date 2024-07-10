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
  //stringDF.writeStream.format("console").start()

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

  //val contactDF = stringDF.withColumn("value", from_json(col("value"), dataSchema))
   // .select(col("value.*"))
 // val parsed_df = stringDF.select(from_json(col("value"), dataSchema).alias("data")).select("data.*")

  //Parsing for the contact from the string DF
  var contactDF = stringDF
    .select(explode(from_json(col("value"), ArrayType(dataSchema))).alias("data"))
    .select("data.*")

  //basic select stmt.
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

//  contactDF = contactDF.withColumn("new1",
//    when(to_date(col("birthday"), "yyyy-MM-dd").isNotNull, to_date(col("birthday"), "yyyy-MM-dd"))
//      .otherwise(lit(null).cast(DateType))
//  )

  private val defaultPhoneNumber = "+901111111111"
  //only the columns having more than 13 char.
   contactDF = contactDF
    .withColumn("phone", when(functions.length(col("phone")) =!= 13, defaultPhoneNumber).otherwise(col("phone")))
 // val ageData = Seq(contactDF.col("birthDay"),contactDF.col("loginDate")).toDF
  /*contactDF = contactDF
    .withColumn("age", datediff(current_date(), col("birthday")))*/
//  val newDataDF =  contactDF.withColumn("new2",to_date(unix_timestamp(contactDF.col("loginDate"), "yyyy-MM-dd").cast("timestamp")))
  //contactDF = contactDF.withColumn("new", date_format(col("birthDay"),"yyyy-MM-dd"))
  //val df2 = contactDF.withColumn(date_format(col("birthDay"),"MM/dd/yyyy"))

  contactDF =  contactDF.withColumn("loginDate",to_date(unix_timestamp(contactDF.col("loginDate"), "dd-MM-yyyy").cast("timestamp")))
  contactDF =  contactDF.withColumn("birthDay",to_date(unix_timestamp(contactDF.col("birthDay"), "dd-MM-yyyy").cast("timestamp")))
  contactDF = contactDF.withColumn("age", (datediff(current_date(), col("birthDay"))/365).cast(IntegerType))

  //Some kind of filter stmt.
    contactDF.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate","true")
    .start()
    .awaitTermination()

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
