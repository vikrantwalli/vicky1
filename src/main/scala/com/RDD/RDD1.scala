package com.RDD

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object RDD1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("rdd operations").setMaster("local")
    val sc = new SparkContext(conf)
    val SqlContext = new SQLContext(sc)
    val a = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val c = sc.parallelize(Array(11, 12, 13, 14, 15))
    val d = a ++ c
    val e = d.collect()
    e.foreach(print)
    print("-----------------------------------------------------   ")
    val f = ArrayBuffer(16, 17, 18, 19, 20)

    f += (21, 22, 23)
    f.foreach(println)
  }


  }
