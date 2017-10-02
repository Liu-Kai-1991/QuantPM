package qpm.data.connection

import java.util.{Calendar, Date}

import MongoDBConnectionImplicits._
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import qpm.data.acquire.finra.regsho.{RegShoRecord, ShortSaleVolume}
import qpm.system.Log

object MongoDBConnectionTest extends App with Log{
  val collection = RegShoRecord.collection
  val docs = collection.find().results
  println(docs.size)
}

object CaseClassTest extends App{

  import org.mongodb.scala.bson.codecs.Macros._
  import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
  import org.bson.codecs.configuration.CodecRegistries.{fromRegistries, fromProviders}

  val codecRegistry = fromRegistries(fromProviders(classOf[RegShoRecord]), DEFAULT_CODEC_REGISTRY )

  val mongoClient: MongoClient = MongoClient("mongodb://192.168.1.21:27018")
  val database: MongoDatabase = mongoClient.getDatabase("test").withCodecRegistry(codecRegistry)
  val collection: MongoCollection[RegShoRecord] = database.getCollection("test")

  val person: RegShoRecord = RegShoRecord(Calendar.getInstance.getTime, "fake", 1, 1, 1, "fake", "fake")
  collection.insertOne(person).printResults()
}