package qpm.data.acquire.finra.regsho

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneId}
import java.util.Date

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.Indexes
import qpm.data.acquire.HttpClientUtil
import qpm.data.connection.MongoDBConnection
import qpm.system.{Log, Storable, StorableCompanion}
import qpm.data.connection.MongoDBConnectionImplicits._

import scala.collection.immutable._

case class RegShoRecord(
  date: Date,
  symbol: String,
  shortVolume: Int,
  shortExemptVolume: Int,
  totalVolume: Int,
  market: String,
  _id: ObjectId) extends Storable

object RegShoRecord extends StorableCompanion[RegShoRecord]{
  def apply(
    date             : Date,
    symbol           : String,
    shortVolume      : Int,
    shortExemptVolume: Int,
    totalVolume      : Int,
    market           : String): RegShoRecord =
    RegShoRecord(date, symbol, shortVolume, shortExemptVolume, totalVolume, market, new ObjectId())

  val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[RegShoRecord]), DEFAULT_CODEC_REGISTRY )
  lazy val collection: MongoCollection[RegShoRecord] =
    MongoDBConnection.getDefaultDatabase.withCodecRegistry(codecRegistry).getCollection(name)
}

object ShortSaleVolume extends Log{
  //http://regsho.finra.org/regsho-Index.html
  private val urlPrefix = "http://regsho.finra.org/"

  private val ADF = "FNRAshvol"
  private val NASDAQ = "FNSQshvol"
  private val NYSE = "FNYXshvol"
  private val ORF = "FORFshvol"
  val exchangeList = Seq(ADF, NASDAQ, NYSE, ORF)

  val defaultTimeZone = "SystemV/EST5"

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def parseWebPage(table: String): Vector[RegShoRecord] = {
    val rows = table.split('\n')
    assert(rows.head == "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market")
    val length = rows.last.toInt
    val res = rows.drop(1).dropRight(1).map{
      row =>
        val elements = row.split('|')
        val date = Date.from(LocalDate.parse(elements(0), dateTimeFormatter).atStartOfDay(ZoneId.systemDefault).toInstant)
        RegShoRecord(date, elements(1), elements(2).toInt, elements(3).toInt, elements(4).toInt, elements(5))
    }.toVector
    assert(res.length == length, "The actual length should equal to indicated length")
    log.info("parse WebPage successfully")
    res
  }

  def getData(source: String, date: LocalDate): (Int, Vector[RegShoRecord]) = {
    val url = s"$urlPrefix$source${date.format(dateTimeFormatter)}.txt"
    val webPage = HttpClientUtil.getWebPage(url)
    if (webPage.good) (webPage.statusCode, parseWebPage(webPage.content)) else
      (webPage.statusCode, Vector())
  }
}
