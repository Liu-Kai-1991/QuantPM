package qpm.data.connection

import MongoDBConnectionImplicits._
import qpm.system.Log

object MongoDBConnectionTest extends App with Log{
  val collection = MongoDBConnection.getCollection("test", "test")
  println(collection.find().headResult.toString)
}
