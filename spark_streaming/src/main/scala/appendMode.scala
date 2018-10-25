import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, explode, split}
import org.apache.spark.sql.hive._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.execution.streaming.StreamingRelation
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};


object appendMode {

  def main(args: Array[String]) {

    val spark = SparkSession.builder.appName("Spark-socket-Integration").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")


//    val schema = StructType(
//      Seq(
//        StructField("Restaurant Name",StringType,true),
//        StructField("City",StringType,true),
//        StructField("Average Cost for two",StringType,true),
//        StructField("Has Online delivery",StringType,true),
//        StructField("Price range",StringType,true),
//        StructField("Aggregate rating",StringType,true)
//      )
//    )

    // Reading a schema form the sample file to avoid writing schema by own
    val dfr = spark.read.option("Header",true).csv("/user/viswatejaster9073/streaming/samplefile.csv")
    val sch= dfr.schema
    println("schema",sch)

    //Creating a streaming Data frame
    val df = spark.readStream.option("header",true).schema(sch).csv("/user/viswatejaster9073/streaming")
   
    // Printing if the dataframe is ready for stream
    println("Is the stream Ready?")
    println(df.isStreaming)


    val df1 = df.select("Restaurant Name","City","Has Online delivery","Price range")

    val finalval = df1.writeStream.outputMode("append").format("console").option("truncate",false)
              .option("numRows",20).start().awaitTermination()
  }
}
