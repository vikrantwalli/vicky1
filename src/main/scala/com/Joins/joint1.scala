package com.Joins

import org.apache.spark.sql.SparkSession

object joint1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("joins in dataframe").master("local").getOrCreate()
/*Joins Supported by Spark
  'inner',
  'outer','full','fullouter','full_outer'
  'leftouter','left','left_outer'
  'rightouter','right','right_outer'
  'leftsemi','left_semi',
  'leftanti','left_anti',
  'cross'*/
    val df1 = spark.read.option("inferSchema",true).option("header",true)
      .csv("C:\\Users\\vikrant\\Desktop\\Test Data\\joinDf1.csv")
    //df1.show()
 /*   +---+--------+---+---------+------+
      | ID|    Name|Age|  Address|Salary|
      +---+--------+---+---------+------+
      |  1|    Ross| 32|Ahmedabad|  2000|
      |  2|  Rachel| 25|    Delhi|  1500|
      |  3|Chandler| 23|     Kota|  2000|
      |  4|  Monika| 25|   Mumbai|  6500|
      |  5|    Mike| 27|   Bhopal|  8500|
      |  6|  Phoebe| 22|       MP|  4500|
      |  7|    Joey| 24|   Indore| 10000|
      +---+--------+---+---------+------+*/

    val df2 = spark.read.option("inferSchema",true).option("header",true)
      .csv("C:\\Users\\vikrant\\Desktop\\Test Data\\joinDf2.csv")
    //df2.show()
 /*   +---+-------------+---+------+
      |OID|         Date| ID|Amount|
      +---+-------------+---+------+
      |102|08-10-16 0:00|  3|  3000|
      |100|08-10-16 0:00|  3|  1500|
      |101|20-11-16 0:00|  2|  1560|
      |103|20-05-15 0:00|  4|  2060|
      +---+-------------+---+------+*/
/*INNER JOIN
* Shows only comman data of the both tables (columns + data)*/
   // df1.join(df2,df1.col("ID") === df2.col("ID"),"inner").show()
    //df1.join(df2).where(df1.col("ID") === df2.col("ID")).show()
    //df1.join(df2).filter(df1("ID") === df2("ID")).show()
  /*  +---+--------+---+-------+------+---+-------------+---+------+
      | ID|    Name|Age|Address|Salary|OID|         Date| ID|Amount|
      +---+--------+---+-------+------+---+-------------+---+------+
      |  2|  Rachel| 25|  Delhi|  1500|101|20-11-16 0:00|  2|  1560|
      |  3|Chandler| 23|   Kota|  2000|100|08-10-16 0:00|  3|  1500|
      |  3|Chandler| 23|   Kota|  2000|102|08-10-16 0:00|  3|  3000|
      |  4|  Monika| 25| Mumbai|  6500|103|20-05-15 0:00|  4|  2060|
      +---+--------+---+-------+------+---+-------------+---+------+*/

 /*LEFT JOIN(Left outer join)
* shows comman data in both tables and all the data of the left side table  */
   // df1.join(df2,df1.col("ID") === df2.col("ID"),"left").show()
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"leftouter").show()
 /*   +---+--------+---+---------+------+----+-------------+----+------+
      | ID|    Name|Age|  Address|Salary| OID|         Date|  ID|Amount|
      +---+--------+---+---------+------+----+-------------+----+------+
      |  1|    Ross| 32|Ahmedabad|  2000|null|         null|null|  null|
      |  2|  Rachel| 25|    Delhi|  1500| 101|20-11-16 0:00|   2|  1560|
      |  3|Chandler| 23|     Kota|  2000| 100|08-10-16 0:00|   3|  1500|
      |  3|Chandler| 23|     Kota|  2000| 102|08-10-16 0:00|   3|  3000|
      |  4|  Monika| 25|   Mumbai|  6500| 103|20-05-15 0:00|   4|  2060|
      |  5|    Mike| 27|   Bhopal|  8500|null|         null|null|  null|
      |  6|  Phoebe| 22|       MP|  4500|null|         null|null|  null|
      |  7|    Joey| 24|   Indore| 10000|null|         null|null|  null|
      +---+--------+---+---------+------+----+-------------+----+------+*/
 /*RIGHT JOIN(Right outer join)
    * shows comman data in both tables and all the data of the right side table  */
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"right").show()
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"rightouter").show()

  /*  +----+--------+----+-------+------+---+-------------+---+------+
      |   3|Chandler|  23|   Kota|  2000|102|08-10-16 0:00|  3|  3000|
      |   3|Chandler|  23|   Kota|  2000|100|08-10-16 0:00|  3|  1500|
      |   2|  Rachel|  25|  Delhi|  1500|101|20-11-16 0:00|  2|  1560|
      |   4|  Monika|  25| Mumbai|  6500|103|20-05-15 0:00|  4|  2060|
      |null|    null|null|   null|  null|104|21-05-15 0:00|  8|  3000|
      +----+--------+----+-------+------+---+-------------+---+------+*/
/*LEFT SEMI JOIN
* leftsemi join is similar to inner join
* difference being leftsemi join returns all columns from the left dataset
* and ignores all columns from the right dataset.
* In other words, this join returns columns from the only left dataset
* for the records match in the right dataset on join expression,
* records not matched on join expression are ignored from both left and right datasets. */

    //df1.join(df2,df1.col("ID") === df2.col("ID"),"leftsemi").show()
/*+---+--------+---+-------+------+
  | ID|    Name|Age|Address|Salary|
  +---+--------+---+-------+------+
  |  2|  Rachel| 25|  Delhi|  1500|
  |  3|Chandler| 23|   Kota|  2000|
  |  4|  Monika| 25| Mumbai|  6500|
  +---+--------+---+-------+------+*/

/*LEFT ANTI JOIN
leftanti join does the exact opposite of the leftsemi,
leftanti join returns only columns from the left dataset for non-matched records.
*/
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"leftanti").show()
/*+---+------+---+---------+------+
  | ID|  Name|Age|  Address|Salary|
  +---+------+---+---------+------+
  |  1|  Ross| 32|Ahmedabad|  2000|
  |  5|  Mike| 27|   Bhopal|  8500|
  |  6|Phoebe| 22|       MP|  4500|
  |  7|  Joey| 24|   Indore| 10000|
  +---+------+---+---------+------+
*/

/*Outer, Full, Fullouter Join
Outer a.k.a full, fullouter join returns all rows from both datasets,
where join expression doesn’t match it returns null on respective record columns.
 */
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"outer").show()
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"full").show()
    //df1.join(df2,df1.col("ID") === df2.col("ID"),"fullouter").show()
   /* +----+--------+----+---------+------+----+-------------+----+------+
      |  ID|    Name| Age|  Address|Salary| OID|         Date|  ID|Amount|
      +----+--------+----+---------+------+----+-------------+----+------+
      |   1|    Ross|  32|Ahmedabad|  2000|null|         null|null|  null|
      |   6|  Phoebe|  22|       MP|  4500|null|         null|null|  null|
      |   3|Chandler|  23|     Kota|  2000| 102|08-10-16 0:00|   3|  3000|
      |   3|Chandler|  23|     Kota|  2000| 100|08-10-16 0:00|   3|  1500|
      |   5|    Mike|  27|   Bhopal|  8500|null|         null|null|  null|
      |   4|  Monika|  25|   Mumbai|  6500| 103|20-05-15 0:00|   4|  2060|
      |null|    null|null|     null|  null| 104|21-05-15 0:00|   8|  3000|
      |   7|    Joey|  24|   Indore| 10000|null|         null|null|  null|
      |   2|  Rachel|  25|    Delhi|  1500| 101|20-11-16 0:00|   2|  1560|
      +----+--------+----+---------+------+----+-------------+----+------+*/
/*cross join
* Cross (or Cartesian) joins (match every row in the left dataset with every row in the right dataset)
* Cross-joins in simplest terms are innerjoins that do not specify a predicate.
* Cross joins will join every single row in the left DataFrame to ever single row in the right DataFrame.
* This will cause an absolute explosion in the number of rows contained in the resulting DataFrame.
* If you have 1,000 rows in each DataFrame, the cross-join of these will result in 1,000,000 (1,000 x 1,000) rows.
* For this reason, you must very explicitly state that you want a cross-join by using the cross join keyword*/
    val joinType= "cross"
    val joinExpression = df1.col("ID") === df2.col("ID")
    df1.join(df2,joinExpression,joinType).show()

/*    +---+--------+---+-------+------+---+-------------+---+------+
      | ID|    Name|Age|Address|Salary|OID|         Date| ID|Amount|
      +---+--------+---+-------+------+---+-------------+---+------+
      |  2|  Rachel| 25|  Delhi|  1500|101|20-11-16 0:00|  2|  1560|
      |  3|Chandler| 23|   Kota|  2000|100|08-10-16 0:00|  3|  1500|
      |  3|Chandler| 23|   Kota|  2000|102|08-10-16 0:00|  3|  3000|
      |  4|  Monika| 25| Mumbai|  6500|103|20-05-15 0:00|  4|  2060|
      +---+--------+---+-------+------+---+-------------+---+------+*/

    val a = spark.sparkContext.parallelize(Array(1,2,3,4,5,6))



  }

}
