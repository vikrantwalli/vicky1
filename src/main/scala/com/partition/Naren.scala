package com.partition

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Naren {
  def main(args: Array[String]): Unit = {
    val Spark = SparkSession.builder().appName("record").master("local").getOrCreate()
   // val a = Spark.read.option("header",true).option("inferschema", true).load("records.txt")
     // a.show('5')

    import Spark.implicits._
    val b = Seq(1,2,3,4,5,6,7,8,9,10).toDF()
    val c = Spark.range(50).toDF()
    b.show()
    c.show(50)



      val rangepart = c.repartition(5)
    println("==========="+rangepart.rdd.getNumPartitions+"=============")
    //rangepart4.write.csv("C:\\Users\\vikrant\\\\Desktop\\Naren\\rangepart5")
      val rangecol3 = rangepart.coalesce(3)
    println("==========="+rangecol3.rdd.getNumPartitions+"=============")
    rangecol3.write.csv("C:\\Users\\vikrant\\\\Desktop\\Naren\\rangecol3")


   val part4 = b.repartition(4)
    //println("==========="+part4.rdd.getNumPartitions+"=============")
    //part4.write.csv("C:\\Users\\vikrant\\\\Desktop\\Naren\\repart4")

  val col2 = b.coalesce(3)
    //println("==========="+col2.rdd.getNumPartitions+"=============")
   // col2.write.csv("C:\\Users\\vikrant\\\\Desktop\\Naren\\col2")
    //val df1 = b.repartition()

  }

}
