package qpm.data.connection

import java.io.File
import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.mongodb.scala.{Document, MongoClient, MongoCollection, Observable}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MongoDBConnection {
  private val defaultDatabase = "qpm"
  private val configPath = "config/MongoDb.conf"
  private val config = ConfigFactory.parseFile(new File(configPath))
  private val serverUrl = config.getString("serverUrl")
  private val mongoClient: MongoClient = MongoClient(serverUrl)
  private val collectionMap: mutable.Map[(String, String), MongoCollection[Document]] = mutable.Map()

  def getCollection(collection: String, database: String): MongoCollection[Document] = {
    val args = (database, collection)
    def createNew = {
      val result = mongoClient.getDatabase(database).getCollection(collection)
      collectionMap.put(args, result)
      result
    }
    if (collectionMap.contains(args)) collectionMap(args) else createNew
  }

  def getCollection(collection: String): MongoCollection[Document] =
    getCollection(defaultDatabase, collection)
}

object MongoDBConnectionImplicits {

  val defaultAwaitTime = Duration(10, TimeUnit.SECONDS)

  implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
    override val converter: (Document) => String = (doc) => doc.toJson
  }

  implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
    override val converter: (C) => String = (doc) => doc.toString
  }

  trait ImplicitObservable[C] {
    val observable: Observable[C]
    val converter: (C) => String

    def results: Seq[C] = Await.result(observable.toFuture(), defaultAwaitTime)
    def headResult: C = Await.result(observable.head(), defaultAwaitTime)
    def printResults(initial: String = ""): Unit = {
      if (initial.length > 0) print(initial)
      results.foreach(res => println(converter(res)))
    }
    def printHeadResult(initial: String = ""): Unit = println(s"$initial${converter(headResult)}")
  }

}