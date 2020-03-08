package com.timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object timeStamp2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("time2").master("local").getOrCreate()
import spark.implicits._

    val df = Seq(("2019-07-01 12:01:19.000"),
      ("2019-06-24 12:01:19.000"),
      ("2019-11-16 16:44:55.406"),
      ("2019-11-16 16:50:59.406")).toDF("input")

    df.withColumn("datetype",to_date(col("input"),"yyyy-MM-dd")).show()
    /*+--------------------+----------+
      |               input|  datetype|
      +--------------------+----------+
      |2019-07-01 12:01:...|2019-07-01|
      |2019-06-24 12:01:...|2019-06-24|
      |2019-11-16 16:44:...|2019-11-16|
      |2019-11-16 16:50:...|2019-11-16|
      +--------------------+----------+*/

    df.withColumn("ts",to_timestamp(col("input")))
      .withColumn("datetype",to_date(col("ts")))
      .show(false)

    df.withColumn("ts",to_timestamp(col("input")))
      .withColumn("datetype",col("ts").cast(DateType))
      .show(false)

    /* +-----------------------+-----------------------+----------+
       |input                  |ts                     |datetype  |
       +-----------------------+-----------------------+----------+
       |2019-07-01 12:01:19.000|2019-07-01 12:01:19    |2019-07-01|
       |2019-06-24 12:01:19.000|2019-06-24 12:01:19    |2019-06-24|
       |2019-11-16 16:44:55.406|2019-11-16 16:44:55.406|2019-11-16|
       |2019-11-16 16:50:59.406|2019-11-16 16:50:59.406|2019-11-16|
       +-----------------------+-----------------------+----------+*/


  }

}
