package com.windoSpark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.SparkEnv


object WindowIng {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("windowing").getOrCreate()

  /*  val df = spark.read.option("inferSchema",true).option("header",true)
      .csv("C:\\Users\\vikrant\\Desktop\\empsalary.csv").toDF()

    df.printSchema()
    df.withColumn("new salary",df("salary")*100).show()
*/
    //df.printSchema()
    /*Shows the sum of salary of each emp addition with privios row*/
   // val window1 = Window.partitionBy("Department").orderBy("FirstName")
    //val df2 = df.withColumn("count",sum("Salary").over(window1)).show()
    /*
    //department wise 2nd highest salary
    val window2 = Window.partitionBy("Department").orderBy(desc("Salary"))

    val rankByDept = rank().over(window2)

    val df4 = df.withColumn("rank",rankByDept)
    df.withColumn("rank",rankByDept).where("rank = 2").drop("rank").show()
    */
    /*
    val window3 = Window.orderBy(asc("FirstName"))
    val row_num = dense_rank().over(window3)
    df.withColumn("row_num",row_num).show()
    */

    val rankDf = spark.read.option("inferSchema",true).option("header",true)
      .csv("C:\\Users\\vikrant\\Desktop\\Test Data\\rankSalary.csv")
    //rankDf.printSchema()
    //rankDf.show()

    val window4 = Window.orderBy(asc("Salary"))

    val row_num = row_number().over(window4)
    val rank1 = rank().over(window4)
    val denceRank = dense_rank().over(window4)
    val percentRank = percent_rank().over(window4)
    rankDf.withColumn("row_num",row_num)//show row numbers according to key as salary 1,2,3,4,n sequence of numbers
      .withColumn("rank",rank1)//show rank numbers according to key as salary 1,1,3,3,5,n skips the n+1 number
      .withColumn("denseRank",denceRank)//show rank numbers according to key as salary 1,1,2,2,3,n show same ranke to common values
      .withColumn("percentRank",percentRank)//Returns the relative rank of a value within a group of values.
                                                      // PERCENT_RANK = (r - 1) / (n - 1)
      .show()

    //assign grade salary wise
    rankDf.withColumn("grade",
       when(col("Salary") === 50000,"Excelent" )
      .when(col("Salary") === 40000,"very good")
      .when(col("Salary") === 30000,"good").otherwise("poor"))
      .orderBy(desc("Salary")).show()
    print(SparkEnv.get)
     //marks <=100 && marks >=90, excelent
  }

}
