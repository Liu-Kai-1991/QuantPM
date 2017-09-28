package qpm.data.acquire.finra.regsho

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import qpm.data.acquire.HttpClientUtil
import qpm.data.acquire.HttpClientUtil.WebPage

import scala.collection.immutable._

case class RegShoRecord(
  date: LocalDate,
  symbol: String,
  shortVolume: Int,
  shortExemptVolume: Int,
  totalVolume: Int,
  market: String)

object ShortSaleVolume {
  //http://regsho.finra.org/regsho-Index.html
  private val urlPrefix = "http://regsho.finra.org/"

  private val ADF = "FNRAshvol"
  private val NASDAQ = "FNSQshvol"
  private val NYSE = "FNYXshvol"
  private val ORF = "FORFshvol"

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

  def parseWebPage(table: String): Vector[RegShoRecord] = {
    val rows = table.split('\n')
    assert(rows.head == "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market")
    val length = rows.last.toInt
    val res = rows.drop(1).dropRight(1).map{
      row =>
        val elements = row.split('|')
        val date = LocalDate.parse(elements(0), dateTimeFormatter)
        RegShoRecord(date, elements(1), elements(2).toInt, elements(3).toInt, elements(4).toInt, elements(5))
    }.toVector
    assert(res.length == length, "The actual length should equal to indicated length")
    res
  }

  def getData(source: String, date: LocalDate): (Int, Vector[RegShoRecord]) = {
    val url = s"$urlPrefix$source${date.format(dateTimeFormatter)}.txt"
    val webPage = HttpClientUtil.getWebPage(url)
    if (webPage.good) (webPage.statusCode, parseWebPage(webPage.content)) else
      (webPage.statusCode, Vector())
    }
}

