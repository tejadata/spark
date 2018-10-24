import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{explode, split,col}
import org.apache.spark.sql.hive._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.sql.execution.streaming.StreamingRelation

object completeMode {


  def main(args: Array[String]) {



    val spark = SparkSession.builder.appName("Spark-socket-Integration").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    val df = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()

//converting Dataframe to Dataset
    import spark.implicits._
    val df1 = df.as[String]

    val words = df1.flatMap(value=>value.split(" "))

    val count = words.groupBy("value").count()


    val outp = count.writeStream.outputMode("complete").format("console").start()

    outp.awaitTermination()

  }
}