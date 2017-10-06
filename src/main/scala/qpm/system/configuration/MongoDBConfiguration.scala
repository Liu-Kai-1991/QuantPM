package qpm.system.configuration

object MongoDBConfiguration extends Configuration{
  val configurationFile = "MongoDb.conf"
  protected val initialized: Init = init()
  val serverUrl: String = config.getString("serverUrl")
}
