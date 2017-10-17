package qpm.data.acquire.finra.regsho

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Date

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Document, MongoCollection}
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import qpm.data.acquire.HttpClientUtil
import qpm.data.connection.MongoDBConnection
import qpm.system.{Log, Storable, StorableCompanion}
import qpm.data.connection.MongoDBConnectionImplicits._

import scala.collection.immutable._
import scala.util.Try

case class RegShoRecord(
  date: Date,
  symbol: String,
  shortVolume: Long,
  shortExemptVolume: Long,
  totalVolume: Long,
  market: String,
  source: String,
  _id: ObjectId) extends Storable

object RegShoRecord extends StorableCompanion[RegShoRecord]{
  def apply(
    date             : Date,
    symbol           : String,
    shortVolume      : Long,
    shortExemptVolume: Long,
    totalVolume      : Long,
    market           : String,
    source           : String): RegShoRecord =
    RegShoRecord(date, symbol, shortVolume, shortExemptVolume, totalVolume, market, source, new ObjectId())

  lazy val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[RegShoRecord]), DEFAULT_CODEC_REGISTRY )
  lazy val collection: MongoCollection[RegShoRecord] =
    MongoDBConnection.getDefaultDatabase.withCodecRegistry(codecRegistry).getCollection(name)
  lazy val rawCollection: MongoCollection[Document] =
    MongoDBConnection.getDefaultDatabase.getCollection(name)
  def countInDateAndMarket(date: Date, market: String): Long =
    collection.count(and(equal("date", date), equal("market", market))).headResult
}

object ShortSaleVolume extends Log{
  //http://regsho.finra.org/regsho-Index.html
  private val urlPrefix = "http://regsho.finra.org/"

  val sourceList = Map("ADF" -> "FNRAshvol", "NASDAQ" -> "FNSQshvol", "NYSE" -> "FNYXshvol", "ORF" -> "FORFshvol")
  private val sourceListReverse = sourceList.map{case (k,v) => (v,k)}

  val defaultTimeZone = "SystemV/EST5"

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  private val head1 = "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market"

  def parseWebPage(table: String, source: String): Vector[RegShoRecord] = {
    val rows = table.split('\n').map(_.trim)
    rows.head match {
      case `head1` =>
        val (checkRows, otherRows) = rows.partition(_.matches("^\\d+$"))
        val recRows = otherRows.filterNot(_ == head1)
        val expectedRecNum = checkRows.map(_.toInt).sum
        assert(expectedRecNum == recRows.length, "The actual length should equal to indicated length")
        recRows.map{
          row =>
            val elements = row.split('|')
            val date = Try{Date.from(LocalDate.parse(elements(0), dateTimeFormatter)
              .atStartOfDay(ZoneId.systemDefault).toInstant)}.toEither match {
              case Left(e) =>
                log.error(s"Date is ${elements(0)}, in row: $row")
                throw e
              case Right(d) => d
            }
            (date, elements(1), elements(2).toLong, elements(3).toLong, elements(4).toLong, elements(5))
        }.groupBy{
          case (date, symbol, _, _, _, market) => (date, symbol, market)
        }.map{
          case ((date, symbol, market), list) =>
            RegShoRecord(date, symbol, list.map(_._3).sum, list.map(_._4).sum, list.map(_._5).sum, market, source)
        }.toVector
      case other =>
        val errorMessage = s"Header not recognized: $other"
        log.error(errorMessage)
        throw new Exception(errorMessage)
    }
  }

  def getData(source: String, date: LocalDate): (Int, Vector[RegShoRecord]) = {
    val url = s"$urlPrefix$source${date.format(dateTimeFormatter)}.txt"
    val webPage = HttpClientUtil.getWebPage(url)
    if (webPage.good) (webPage.statusCode, parseWebPage(webPage.content, sourceListReverse(source))) else
      (webPage.statusCode, Vector())
  }
}
