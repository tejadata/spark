
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{udf,current_timestamp,col}
import org.apache.spark.sql.types.{StringType}




object timeStamp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Spark-socket-Integration").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    // Reading a schema form the sample file to avoid writing schema by own
    val dfr = spark.read.option("Header", true).csv("/user/viswatejaster9073/samplefile.csv")
    val sch = dfr.schema
    println("schema", sch)

    //Creating a streaming Data frame
    val df = spark.readStream.option("header", false).schema(sch).csv("/user/viswatejaster9073/streaming")
    println("Is the stream Ready?")
    println(df.isStreaming)

     //function to return date time as specifc format as sting

    def current_time = udf(() => {
      java.time.LocalDateTime.now().toString
    })


    import spark.implicits._

    val dfwithtime = df.withColumn("current_timestamp3", current_time())

    val dff = dfwithtime.select("Restaurant Name","City","current_timestamp3")

    val query = dff.writeStream.outputMode("append").format("console").option("truncate",false)
      .option("numRows",20).start().awaitTermination()


  }
}
