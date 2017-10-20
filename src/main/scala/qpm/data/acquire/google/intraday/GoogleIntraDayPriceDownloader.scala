package qpm.data.acquire.google.intraday

import java.time.{Instant, ZoneId}

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
  val symbolPriorityMap = GoogleIntraDay.symbolPriorityMap
  val tasks = symbolList.map{
    symbol =>
      val googleSymbol = NasdaqSymbol(symbol).toGoogleFinanceSymbol
      (googleSymbol, symbolPriorityMap.getOrElse(googleSymbol, Int.MaxValue))
  }.sortBy(- _._2)

  tasks.foreach{
    case (googleSymbol, priority) =>
      val currentHour = Instant.now.atZone(estZone).getHour
      if (currentHour <= 7 || currentHour >= 18) {
        Thread.sleep(5000 + Random.nextInt(5000))
        log.info(s"Start working with $googleSymbol (priority $priority), current hour $currentHour, numDays ${cmdLine.numDays}")
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
