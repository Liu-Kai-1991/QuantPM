package qpm.data.acquire.google.intraday

import java.util.concurrent.Executors

import org.kohsuke.args4j.{Option => CmdOption}
import org.mongodb.scala.model.Filters
import qpm.data.acquire.nasdaq.companies.NasdaqCompanyInfo
import qpm.system.{Log, MultiThreadCmdLine, QuantPMApp, QuantPMCmdLine}
import org.mongodb.scala.model.Sorts._
import qpm.data.connection.MongoDBConnectionImplicits._
import qpm.data.general.NasdaqSymbol

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try

object GoogleIntraDayPriceDownloaderCmdLine extends QuantPMCmdLine with MultiThreadCmdLine{
  @CmdOption(name = "-n", required = false, usage = "number of days")
  var numDays: Int = 1
}


object GoogleIntraDayPriceDownloader extends QuantPMApp(GoogleIntraDayPriceDownloaderCmdLine) with Log{

  val numWorkers = sys.runtime.availableProcessors
  log.info(s"Number of threads $numWorkers")
  val pool = Executors.newFixedThreadPool(numWorkers)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)


  val symbolList = NasdaqCompanyInfo.collection.find().sort(descending("timeStamp")).limit(1)
    .headResult.records.map(_.symbol)
  val tasks = symbolList
  val resultFutures = tasks.map{
    symbol =>
      val googleSymbol = NasdaqSymbol(symbol).toGoogleFinanceSymbol
      Try(IntraDayPrice.getData(googleSymbol, 1)).toEither match {
        case Left(e) =>
          log.error(s"Download data for $googleSymbol failed")
          e.getStackTrace.foreach(trace => log.error(trace.toString))
          Future(Seq())
        case Right((statusCode,  seq)) =>
          if (statusCode>= 300){
            log.error(s"Download data for $googleSymbol failed with code $statusCode")
            Future(Seq())
          } else {
            val resultFutures = seq.map{
              googleIntraDay =>
                GoogleIntraDay.putIfNotExsist(googleIntraDay, Filters.and(
                  Filters.eq("symbol", googleIntraDay.symbol),
                  Filters.eq("date", googleIntraDay.date)
                ))
            }
            val futureResults = Future.sequence(resultFutures).map(_.flatten)
            futureResults
          }
      }
  }
  val futureResults = Future.sequence(resultFutures).map(_.flatten)
  val results = Await.result(futureResults, Duration.Inf)
  Await.result(Future.sequence(results.map{
    result =>
      result.toFuture.map{
        complete =>
          println(complete)
          complete
      }
  }), Duration.Inf)
}
