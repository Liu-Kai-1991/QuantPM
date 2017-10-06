package qpm.data.connection

import java.util.concurrent.TimeUnit

import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, Observable}
import qpm.system.configuration.MongoDBConfiguration

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object MongoDBConnection {
  private val defaultDatabase = "qpm"
  private val serverUrl = MongoDBConfiguration.serverUrl
  private val mongoClient: MongoClient = MongoClient(serverUrl)

  def getDatabase(dbName: String): MongoDatabase = mongoClient.getDatabase(dbName)

  def getDefaultDatabase: MongoDatabase = getDatabase(defaultDatabase)

  def getCollection(
  collection: String,
  database: String): MongoCollection[Document] = mongoClient.getDatabase(database).getCollection(collection)

  def getCollection(collection: String, codecRegistry: Option[CodecRegistry] = None): MongoCollection[Document] =
    getCollection(defaultDatabase, collection)
}

object MongoDBConnectionImplicits {

  val defaultAwaitTime = Duration(30, TimeUnit.SECONDS)

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