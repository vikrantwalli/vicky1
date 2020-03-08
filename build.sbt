name := "v2Mysql"

version := "0.1"

scalaVersion := "2.11.1"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"
//libraryDependencies += "org.rogach" %% "scallop" % "2.3.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.24"