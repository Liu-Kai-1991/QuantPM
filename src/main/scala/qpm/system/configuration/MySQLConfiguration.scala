package qpm.system.configuration

import qpm.system.configuration.MongoDBConfiguration.init

object MySQLConfiguration extends Configuration{
  val configurationFile = "MySql.conf"
  protected val initialized: Init = init()

  val serverUrl: String = config.getString("serverUrl")
  val username: String = config.getString("username")
  val password: String = config.getString("password")
}
