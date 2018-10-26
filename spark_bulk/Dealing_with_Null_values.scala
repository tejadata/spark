
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.fs._
import org.apache.spark.sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{expr,col,pow}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


val conf = new SparkConf().setAppName("Flight status")
val sc = new SparkContext(conf)
val spark = new SQLContext(sc) 

val retail_t = spark.read.format("csv").
            option("header","true").
option("delimiter", ",").
option("inferSchema", "true").
csv("/user/viswatejaster9073/nullvalues/Retail_Data_Transactions_null.csv")

retail_t.summary().show()

retail_t.count()

retail_t.show(10)

retail_t.map(x=>x.anyNull).show(7)

retail_t.map(x=>x.anyNull).filter($"value" === true).count

retail_t.filter($"tran_amount".isNull).count()

var mean =  retail_t.select(avg($"tran_amount")).first().toString()

var mean =  retail_t.select(avg($"tran_amount")).first().toString().replaceAll("[\\[\\]]","")

val retail_fill = retail_t.na.fill(mean.toFloat,Seq("tran_amount"))

retail_fill.show()

val trans_date = retail_t.select($"trans_date").groupBy($"trans_date").count().orderBy($"count".desc).first()(0).toString.replaceAll("[\\[\\]]","")

val customer = retail_t.select($"customer_id").groupBy($"customer_id").count().orderBy($"count".desc).first()(0).toString.replaceAll("[\\[\\]]","")

val fillmissvalues = Map( "customer_id" -> customer,
                          "trans_date" -> trans_date,
                           "tran_amount" -> mean     // Mean we already calculated
                            )

val fillmiss = retail_t.na.fill(fillmissvalues)

fillmiss.show()

retail_t.count()

retail_t.na.drop().count()

retail_t.count()-retail_t.na.drop().count()

retail_t.na.drop("all", Seq("customer_id", "tran_amount")).count()

retail_t.select($"customer_id",$"tran_amount").groupBy("customer_id").sum("tran_amount").show()

retail_t.select($"customer_id",$"tran_amount").groupBy("customer_id").sum().show()

retail_t.dtypes

def s() = for (x <- retail_t.dtypes)
{
    if(x._2 == "IntegerType")
    {
        println(x._1)
        val retail_f1 = retail_t.na.fill(mean.toFloat,Seq(x._1))
       // retail_f.show()

    }
    else
    {
        val retail_f1 = retail_t.na.fill("abc",Seq(x._1))
      // retail_f1.show()
        println(x._1)

    }
}

s

retail_f1

spark.range(1).select(unix_timestamp(time: Column, format: String)).show

Seq("2017-01-01 00:00:00").toDF("time").first

Seq("2017-01-01 00:00:00").toDF("time").withColumn("current_timestamp()", unix_timestamp($"time")).show

import java.util.Date 
import java.text.SimpleDateFormat

val times = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date).toString

print(times)
