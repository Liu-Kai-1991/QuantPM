package qpm.data.acquire.google.intraday

import java.time.{Instant, LocalDate, ZoneId}
import java.util.Date

import com.mongodb.client.model.Aggregates
import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.{Document, MongoCollection}
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import org.mongodb.scala.model.{Accumulators, Filters, Projections, Sorts}
import qpm.data.connection.{HttpClientUtil, MongoDBConnection}
import qpm.data.general.GoogleFinanceSymbol
import qpm.data.general.DateUtil._
import qpm.data.infra.{Storable, StorableCompanion}
import qpm.data.connection.MongoDBConnectionImplicits._

case class GoogleIntraDay(
  symbol: String,
  exchange: String,
  marketOpenMinute: Int,
  marketCloseMinute: Int,
  timeZoneOffset: Int,
  initialTimeStamp: Long,
  date: Date,
  minutes: List[Int],
  closePrices: List[Double],
  highPrices: List[Double],
  lowPrices: List[Double],
  openPrices: List[Double],
  volumes: List[Long],
  _id: ObjectId) extends Storable

object GoogleIntraDay extends StorableCompanion[GoogleIntraDay]{
  def apply(
    symbol           : String,
    exchange         : String,
    marketOpenMinute : Int,
    marketCloseMinute: Int,
    timeZoneOffset   : Int,
    initialTimeStamp : Long,
    date             : Date,
    minutes          : List[Int],
    closePrices      : List[Double],
    highPrices       : List[Double],
    lowPrices        : List[Double],
    openPrices       : List[Double],
    volumes          : List[Long]): GoogleIntraDay =
    new GoogleIntraDay(symbol, exchange, marketOpenMinute, marketCloseMinute, timeZoneOffset, initialTimeStamp, date,
      minutes, closePrices, highPrices, lowPrices, openPrices, volumes, new ObjectId())

  lazy val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[GoogleIntraDay]), DEFAULT_CODEC_REGISTRY)
  lazy val collection: MongoCollection[GoogleIntraDay] =
    MongoDBConnection.getDefaultDatabase.withCodecRegistry(codecRegistry).getCollection(name)
  lazy val rawCollection: MongoCollection[Document] =
    MongoDBConnection.getDefaultDatabase.getCollection(name)

  def symbolPriorityMap: Map[GoogleFinanceSymbol, Int] = {
    val qryRes = rawCollection.aggregate(List(
      Aggregates.group("$symbol", Accumulators.max("maxDate", "$date")))).results
    val currentMilli = Instant.now.toEpochMilli
    qryRes.map(doc => doc.getString("_id") -> doc.getDate("maxDate")).map{
      case (symbol, date) =>
        GoogleFinanceSymbol(symbol) -> ((currentMilli - date.toInstant.toEpochMilli)/3600000).toInt
    }.toMap
  }

  def symbolNewestDateMap(zoneId: ZoneId): Map[GoogleFinanceSymbol, LocalDate] = {
    val qryRes = rawCollection.aggregate(List(
      Aggregates.group("$symbol", Accumulators.max("maxDate", "$date")))).results
    qryRes.map(doc => doc.getString("_id") -> doc.getDate("maxDate")).map{
      case (symbol, date) =>
        GoogleFinanceSymbol(symbol) -> date.toInstant.atZone(zoneId).toLocalDate
    }.toMap
  }
}

object IntraDayPrice {
  
  private val domainList = new {
    private val list = Vector("co.uk", "hk", "cn", "ca", "com")
    private var pointer = list.iterator

    def nextDomain: String =
      if (pointer.hasNext) pointer.next() else {
        pointer = list.iterator
        pointer.next()
      }
  }

  private def getUrl(symbol: GoogleFinanceSymbol, numDays: Int) =
    s"https://finance.google.${domainList.nextDomain}/finance/getprices?q=$symbol&p=${numDays}d&f=d,c,v,k,o,h,l&i=60"

  private case class HeadData(
    exchange: String,
    marketOpenMinute: Int,
    marketCloseMinute: Int,
    timeZoneOffset: Int)

  def extractString(line: String, prefix: String, suffix: String): Option[String] = {
    s"$prefix(.*)$suffix".r.findFirstMatchIn(line).map(_.group(1))
  }

  private def parseHead(head: String): HeadData = {
    val exchange = extractString(head, "EXCHANGE%3D", "\n")
      .getOrElse(throw new MatchError(s"Match EXCHANGE faild for $head"))
    val marketOpenMinute = extractString(head, "MARKET_OPEN_MINUTE=", "\n")
      .getOrElse(throw new MatchError(s"Match MARKET_OPEN_MINUTE faild for $head")).toInt
    val marketCloseMinute = extractString(head, "MARKET_CLOSE_MINUTE=", "\n")
      .getOrElse(throw new MatchError(s"Match MARKET_CLOSE_MINUTE faild for $head")).toInt
    val timeZoneOffset = extractString(head, "TIMEZONE_OFFSET=", "\n")
      .getOrElse(throw new MatchError(s"Match TIMEZONE_OFFSET faild for $head")).toInt
    val columns = extractString(head, "COLUMNS=", "\n")
      .getOrElse(throw new MatchError(s"Match COLUMNS faild for $head"))
    assert(columns == "DATE,CLOSE,HIGH,LOW,OPEN,VOLUME,CDAYS", s"unexpected column: $columns")
    HeadData(exchange, marketOpenMinute, marketCloseMinute, timeZoneOffset)
  }

  private val estZone = ZoneId.of("US/Eastern")
  private def estZoneEodYesterday = Instant.now.minusDays(1).toEodDate(estZone)

  private def parseData(data: String, head: HeadData, symbol: GoogleFinanceSymbol): GoogleIntraDay = {
    val rowElements = data.split('\n').map(_.split(',')).toList
    val initialTimeStamp = rowElements.head.head.toLong
    val initialInstant = new Date(initialTimeStamp * 1000).toInstant
    val localDate = initialInstant.toEodDate(estZone)
    val marketStartTimeStamp = (initialTimeStamp + head.timeZoneOffset * 60)/86400*86400 +
      head.marketOpenMinute * 60 - head.timeZoneOffset * 60
    val minutes = ((initialTimeStamp - marketStartTimeStamp)/60).toInt +: rowElements.drop(1).map(_(0).toInt)
    val closePrices = rowElements.map(_(1).toDouble)
    val highPrices = rowElements.map(_(2).toDouble)
    val lowPrices = rowElements.map(_(3).toDouble)
    val openPrices = rowElements.map(_(4).toDouble)
    val volumes = rowElements.map(_(5).toLong)
    GoogleIntraDay(symbol.symbol, head.exchange, head.marketOpenMinute, head.marketCloseMinute, head.timeZoneOffset,
      initialTimeStamp, localDate, minutes, closePrices, highPrices, lowPrices, openPrices, volumes)
  }

  private def emptyData(symbol: GoogleFinanceSymbol): GoogleIntraDay = {
    GoogleIntraDay(symbol.symbol, "", -1, -1, -1,
      -1, estZoneEodYesterday, List(), List(), List(), List(), List(), List())
  }

  def parseResponse(content: String, symbol: GoogleFinanceSymbol): Seq[GoogleIntraDay] = {
    val infos = content.split('a')
    if (infos.length == 1) {
      Seq(emptyData(symbol))
    } else {
      val headData = parseHead(infos.head)
      infos.drop(1).map(data => parseData(data, headData, symbol))
    }
  }

  def getData(symbol: GoogleFinanceSymbol, numDays: Int): (Int, Seq[GoogleIntraDay]) = {
    val webPage = HttpClientUtil.getWebPage(getUrl(symbol, numDays))
    if (webPage.good) (webPage.statusCode, parseResponse(webPage.content, symbol)) else
      (webPage.statusCode, Seq())
  }
}

object Test extends App{
  println(IntraDayPrice.getData(GoogleFinanceSymbol("ALP^Q"), 1))
}