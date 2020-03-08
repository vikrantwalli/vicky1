package com.timestamp

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object timeStampTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("timeStamp").master("local").getOrCreate()

    val df1 = spark.read.option("header",true).option("inferSchema",true).option("delimiter",",").csv("time.csv ")

    //df1.show()

//    df1.withColumn("FormatedDate",functions
//      .date_format(functions.col("date"),"yyyy-MM-dd")).show()

  /*  df1.withColumn("formatted_date",
      functions.date_format(expr("CAST(date/1000 AS TIMESTAMP)"), "yyyy-MM-dd"))
      .show()
*/
  //  df1.withColumn("date_time", df1("date").cast(DateType)).show()
    //df1.withColumn("date_time", df1("date").cast(StringType).cast(DateType)).show()

/*
    val a = df1.withColumn("date_time",df1("date").cast(StringType)).drop("date").toDF()
    df1.printSchema()
    a.printSchema()

    a.withColumn("fomatedDateTime",a("date_time").cast(DateType)).show()
*/



//    val df2 = spark.range(2).select(current_date(),current_timestamp()).show()
    //df2.printSchema()
    /*
    root
       |-- current_date(): date (nullable = false)
       |-- current_timestamp(): timestamp (nullable = false)
     */
   // val df = df1
     // .withColumn("Time", $"Timestamp".cast("timestamp")).show()

    //val d = df1.withColumn("Date", $"Time".cast("date"))
//      .withColumn("Date", $"Time".cast("date"))
//      .withColumn("EventTime", date_format($"Time", "H:m:s"))

    val colname = df1.columns
    println(colname)


  }

}
