package qpm.system

import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.{Completed, MongoCollection, SingleObservable}

import scala.collection.immutable._

trait StorableCompanion[T <: Storable] {
  val name: String = this.getClass.getName.dropRight(1)
  def collection: MongoCollection[T]
  def codecRegistry: CodecRegistry
  def put(regShoRecord: T): SingleObservable[Completed] =
    collection.insertOne(regShoRecord)
  def putMany(regShoRecords: Seq[T]): SingleObservable[Completed] =
    collection.insertMany(regShoRecords)
}

trait Storable{
  def _id: ObjectId
}
