import java.sql.{Connection, DriverManager}

object exp2 {

  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.43.88:3306/vicky"
    val username = "root"
    val password = "root"

    var connection:Connection = null
    try{
        Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT city_id, city FROM city")
        while ( resultSet.next() ) {
          val city_id = resultSet.getString("city_id")
          val city = resultSet.getString("city")
          println("city_id, city = " + city_id + ", " + city)
        }
    }
    catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
