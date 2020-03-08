import java.sql.DriverManager

import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SparkSession

object MysqlExp {
  def main(args: Array[String]): Unit = {
  //  val spark = SparkSession.builder().master("local").appName("mysql_connection and dataframe").getOrCreate()

  /*  val df = spark.read.format("jdbc").option("url","jdbc:mysql:localhost//3306/sakila")
      .option("driver","com.mysql.jdbc.driver").option("dbtable","city")
      .option("user","root").option("password","root").load()

*/

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost/sakila"
    val username = "root"
    val password = "root"

    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM city")
      while (resultSet.next()) {
        val city_id = resultSet.getString("city_id")
        val city = resultSet.getString("city")
        println("city_id, city = " + city_id + ", " + city)
      }
    }

      catch {
        case e => e.printStackTrace
      }
      connection.close()


  }
}
