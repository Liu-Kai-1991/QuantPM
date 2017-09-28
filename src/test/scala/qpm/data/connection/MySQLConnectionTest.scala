package qpm.data.connection

object MySQLConnectionTest extends App{
  val connection = MySQLConnection("sys")
  val resultSet = connection.executeQuery("SELECT * FROM sys_config")
  while ( resultSet.next() ) {
    println(resultSet.getString(4))
  }
  connection.close()
}
