package qpm.data.infra

import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.model.InsertManyOptions
import org.mongodb.scala.{Completed, MongoCollection, SingleObservable}

import scala.collection.immutable._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

trait StorableCompanion[T <: Storable] {
  private val insertManyOptions = InsertManyOptions().ordered(false)

  val name: String = this.getClass.getName.dropRight(1)
  def collection: MongoCollection[T]
  def codecRegistry: CodecRegistry
  def put(storable: T): SingleObservable[Completed] =
    collection.insertOne(storable)
  def putMany(storables: Seq[T]): SingleObservable[Completed] =
    collection.insertMany(storables, insertManyOptions)
  def checkExsist(filter: Bson)(implicit ct: ClassTag[T], ec: ExecutionContext): Future[Boolean] =
    collection.find(filter).limit(1).toFuture.map(_.nonEmpty)
  def putIfNotExsist(storable: T, filter: Bson)(implicit ct: ClassTag[T], ec: ExecutionContext):
  Future[Option[SingleObservable[Completed]]] =
    checkExsist(filter).map{
      case true => None
      case false => Some(collection.insertOne(storable))
    }
}

trait Storable{
  def _id: ObjectId
}
