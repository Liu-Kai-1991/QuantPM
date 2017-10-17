package qpm.system

import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.InsertManyOptions
import org.mongodb.scala.{Completed, MongoCollection, SingleObservable}

import scala.collection.immutable._

trait StorableCompanion[T <: Storable] {
  private val insertManyOptions = InsertManyOptions().ordered(false)

  val name: String = this.getClass.getName.dropRight(1)
  def collection: MongoCollection[T]
  def codecRegistry: CodecRegistry
  def put(storable: T): SingleObservable[Completed] =
    collection.insertOne(storable)
  def putMany(storables: Seq[T]): SingleObservable[Completed] =
    collection.insertMany(storables, insertManyOptions)
}

trait Storable{
  def _id: ObjectId
}
