package qpm.data.acquire.google.intraday

import java.time.{DayOfWeek, Instant, LocalDate, ZoneId}

import org.kohsuke.args4j.{Option => CmdOption}
import org.mongodb.scala.model.Filters
import qpm.data.acquire.nasdaq.companies.NasdaqCompanyInfo
import org.mongodb.scala.model.Sorts._
import qpm.data.connection.MongoDBConnectionImplicits._
import qpm.data.general.NasdaqSymbol
import qpm.system._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Random, Try}

object GoogleIntraDayPriceDownloaderCmdLine extends QuantPMCmdLine{
  @CmdOption(name = "-n", required = false, usage = "number of days")
  var numDays: Int = 10
}

object GoogleIntraDayPriceDownloader extends QuantPMApp(GoogleIntraDayPriceDownloaderCmdLine) with Log{

  private val estZone = ZoneId.of("US/Eastern")

  val symbolList = NasdaqCompanyInfo.collection.find().sort(descending("timeStamp")).limit(1)
    .headResult.records.map(_.symbol)
  val symbolNewestDateMap = GoogleIntraDay.symbolNewestDateMap(estZone)
  val tasks = symbolList.map{
    symbol =>
      val googleSymbol = NasdaqSymbol(symbol).toGoogleFinanceSymbol
      (googleSymbol, symbolNewestDateMap.getOrElse(googleSymbol, LocalDate.MIN))
  }.sortBy(_._2.toEpochDay)

  def checkDateTime(maxDate: LocalDate): Boolean = {
    val currentZonedDateTime = Instant.now.atZone(estZone)
    val currentHour = currentZonedDateTime.getHour
    val currentWeekday = currentZonedDateTime.getDayOfWeek
    val currentDate = currentZonedDateTime.toLocalDate
    currentWeekday match {
      case DayOfWeek.SATURDAY => currentDate.minusDays(1).isAfter(maxDate)
      case DayOfWeek.SUNDAY => currentDate.minusDays(2).isAfter(maxDate)
      case others => (currentHour <= 7 || currentHour >= 18) && currentDate.isAfter(maxDate)
    }
  }

  tasks.zipWithIndex.foreach{
    case ((googleSymbol, maxDate), taskIndex) =>
      if (checkDateTime(maxDate) ) {
        Thread.sleep(5000 + Random.nextInt(5000))
        log.info(s"Start working with $taskIndex task: $googleSymbol (maxDate $maxDate), numDays ${cmdLine.numDays}")
        Try(IntraDayPrice.getData(googleSymbol, cmdLine.numDays)).toEither match {
          case Left(e) =>
            log.error(s"Download data for $googleSymbol failed")
            e.getStackTrace.foreach(trace => log.error(trace.toString))
          case Right((statusCode,  seq)) =>
            if (statusCode>= 300){
              log.error(s"Download data for $googleSymbol failed with code $statusCode")
            } else {
              log.info(s"Download data for $googleSymbol good")
              seq.foreach{
                googleIntraDay =>
                  val res = GoogleIntraDay.putIfNotExsist(googleIntraDay, Filters.and(
                    Filters.eq("symbol", googleIntraDay.symbol),
                    Filters.eq("date", googleIntraDay.date)
                  )).block.map(_.toFuture.block)
                  log.info(res.toString)
              }
            }
        }
      }
  }
}
