package qpm.data.connection

import java.sql.{DriverManager, ResultSet}

import qpm.system.configuration.MySQLConfiguration

import scala.collection.mutable

class MySQLConnection private(
  private val args: (String /* url */ , String /* username */ , String /* password */ )
) {
  private val connection = DriverManager.getConnection(args._1, args._2, args._3)
  private val statement = connection.createStatement()

  def executeQuery(qry: String): ResultSet =
    statement.executeQuery(qry)

  def close(): Unit = connection.close()
  def isClosed: Boolean = connection.isClosed
}


object MySQLConnection {
  private val driver = "com.mysql.cj.jdbc.Driver"
  private val useUnicode = "useUnicode=true"
  private val useJDBCCompliantTimezoneShift = "useJDBCCompliantTimezoneShift=true"
  private val useLegacyDatetimeCode = "useLegacyDatetimeCode=false"
  private val serverTimezone = "serverTimezone=UTC"
  private val connectionMap: mutable.Map[(String, String, String), MySQLConnection] = mutable.Map()
  private val serverUrl = MySQLConfiguration.serverUrl
  private val username = MySQLConfiguration.username
  private val password = MySQLConfiguration.password

  def apply(database: String): MySQLConnection = {
    val url = s"jdbc:mysql://$serverUrl/$database?$useUnicode&$useJDBCCompliantTimezoneShift&" +
      s"$useLegacyDatetimeCode&$serverTimezone"
    val args = (url, username, password)
    def createNew = {
      val connection = new MySQLConnection(args)
      connectionMap.put(args, connection)
      connection
    }
    if (connectionMap.contains(args)){
      val connection = connectionMap(args)
      if (connection.isClosed) {
        connectionMap.remove(args)
        createNew
      } else connection
    } else createNew
  }

  def close(database: String): Unit = {
    val keys = connectionMap.keys.filter(_._1 == database)
    connectionMap.filterKeys(_._1 == database).values.foreach(_.close())
    keys.foreach(connectionMap.remove)
  }
}