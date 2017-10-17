package qpm.data.acquire.nasdaq.companies

import java.time.LocalDate
import java.util.{Calendar, Date}

import org.bson.codecs.configuration.CodecRegistries.{fromProviders, fromRegistries}
import org.bson.codecs.configuration.CodecRegistry
import org.mongodb.scala.MongoCollection
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.bson.codecs.DEFAULT_CODEC_REGISTRY
import org.mongodb.scala.bson.codecs.Macros._
import qpm.data.acquire.HttpClientUtil
import qpm.data.connection.MongoDBConnection
import qpm.system.{Log, Storable, StorableCompanion}

import scala.util.Try
import scala.collection.immutable._

case class NasdaqCompanyInfo(
  timeStamp: Date,
  timeZone: String,
  rawTimeZoneOffset: Long,
  records: List[NasdaqCompanyRecord],
  _id: ObjectId
) extends Storable

object NasdaqCompanyInfo extends StorableCompanion[NasdaqCompanyInfo]{
  def apply(
    timeStamp        : Date,
    timeZone         : String,
    rawTimeZoneOffset: Long,
    records          : List[NasdaqCompanyRecord]
  ): NasdaqCompanyInfo = new NasdaqCompanyInfo(timeStamp, timeZone, rawTimeZoneOffset, records, new ObjectId())

  lazy val codecRegistry: CodecRegistry = fromRegistries(fromProviders(classOf[NasdaqCompanyInfo],
    classOf[NasdaqCompanyRecord]), DEFAULT_CODEC_REGISTRY )
  lazy val collection: MongoCollection[NasdaqCompanyInfo] =
    MongoDBConnection.getDefaultDatabase.withCodecRegistry(codecRegistry).getCollection(name)
}

case class NasdaqCompanyRecord(
  symbol: String,
  name: String,
  lastSale: Option[Double],
  marketCap: Option[Double],
  adrTso: Option[Double],
  country: Option[String],
  ipoYear: Option[Int],
  sector: Option[String],
  industry: Option[String]
)

object CompanyList extends Log{
  //@deprecated val byIndustry = "http://www.nasdaq.com/screening/companies-by-industry.aspx?render=download"
  val byRegion = "http://www.nasdaq.com/screening/companies-by-region.aspx?render=download"
  val headRegion1 = Vector("Symbol", "Name", "LastSale", "MarketCap", "ADR TSO", "Country",
    "IPOyear", "Sector", "Industry", "Summary Quote")

  def parseByRegion(table: String): NasdaqCompanyInfo = {
    val rows = table.split('\n').map(_.trim).map(_.drop(1).dropRight(2).split("""","""").map(_.trim).toVector)
    val records = rows.head match {
      case `headRegion1` => rows.drop(1).toList.map{
        case Vector(symbol, name, lastSale, marketCap, adrTso, country, ipoYear, sector, industry, _) =>
          NasdaqCompanyRecord(
            symbol,
            name,
            Try(lastSale.toDouble).toOption,
            Try(marketCap.toDouble).toOption,
            Try(adrTso.toDouble).toOption,
            if (country == "n/a") None else Some(country),
            Try(ipoYear.toInt).toOption,
            if (sector == "n/a") None else Some(sector),
            if (industry == "n/a") None else Some(industry))
        }
      case other =>
        val errorMessage = s"Header not recognized: $other"
        log.error(s"expect $headRegion1")
        log.error(s"actual ${rows.head}")
        log.error(errorMessage)
        throw new Exception(errorMessage)
    }
    val calendar = Calendar.getInstance
    val timeStamp = calendar.getTime
    val timeZone = calendar.getTimeZone
    val rawTimeZoneOffset = timeZone.getRawOffset
    NasdaqCompanyInfo(timeStamp, timeZone.getDisplayName, rawTimeZoneOffset, records)
  }

  def getData(url: String): (Int, Option[NasdaqCompanyInfo]) = {
    val webPage = HttpClientUtil.getWebPage(url)
    if (webPage.good) (webPage.statusCode, Some(parseByRegion(webPage.content))) else
      (webPage.statusCode, None)
  }
}