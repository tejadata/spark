
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{udf,current_timestamp,col,desc}
import org.apache.spark.sql.types.{StringType,StructType, StructField,IntegerType}

object timeStamp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("Spark-socket-Integration").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    // Reading a schema form the sample file to avoid writing schema by own

        val schema = StructType(
          Seq(
            StructField("Restaurant Name",StringType,true),
            StructField("City",StringType,true),
            StructField("Average Cost for two",StringType,true),
            StructField("Has Online delivery",StringType,true),
            StructField("Price range",IntegerType,true),
            StructField("Aggregate rating",StringType,true)
          )
        )

    //Creating a streaming Data frame
    val df = spark.readStream.option("header", false).schema(schema).csv("/user/viswatejaster9073/streaming")
    println("Is the stream Ready?")
    println(df.isStreaming)

     //function to return date time as specifc format as sting

    def current_time = udf(() => {
      java.time.LocalDateTime.now().toString
    })


    import spark.implicits._

    val dfwithtime = df.withColumn("current_timestamp3", current_time())

    val dff = dfwithtime.select("Restaurant Name","City","current_timestamp3","Price range")

// calulcaitng the sum of price range using Grouping by for timestamp column
    val g = dff.groupBy("current_timestamp3").sum("Price range").withColumnRenamed("sum(Price range)", "price_total").orderBy(desc("price_total"))


    val query = g.writeStream.outputMode("complete").format("console").option("truncate",false)
      .option("numRows",20).start().awaitTermination()


  }
}
