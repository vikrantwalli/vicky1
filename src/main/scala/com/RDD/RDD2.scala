package com.RDD

import org.apache.spark.{SparkConf, SparkContext}

object RDD2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("read text file in rdd").setMaster("local")
    val sc = new SparkContext(conf)
//Spark Read all text files from a directory into a single RDD
    //val r1 = sc.textFile("C:\\Users\\vikrant\\Desktop\\RDD\\*")
    //r1.foreach(f => {println(f)})
  /*One,1
    Two,2
    Three,3
    Four,4*/

    //val r2 = sc.wholeTextFiles("C:\\Users\\vikrant\\Desktop\\RDD\\*")
    //r2.foreach(f => {println(f._1 +"=>"+ f._2)})
  /*file:/C:/Users/vikrant/Desktop/RDD/text1.txt=>one,1
  file:/C:/Users/vikrant/Desktop/RDD/text2.txt=>two,2
  file:/C:/Users/vikrant/Desktop/RDD/text3.txt=>three,3*/

    //Spark Read multiple text files into a single RDD
    val r3 = sc.textFile("C:\\Users\\vikrant\\Desktop\\RDD\\text1.txt,C:\\Users\\vikrant\\Desktop\\RDD\\text2.txt")
    r3.foreach(f => {println(f)})
  }  
}
