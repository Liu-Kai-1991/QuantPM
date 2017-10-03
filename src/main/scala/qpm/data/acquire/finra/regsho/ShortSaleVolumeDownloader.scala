package qpm.data.acquire.finra.regsho

import java.time.LocalDate

import qpm.system._
import org.kohsuke.args4j.{Option => CmdOption}
import qpm.data.connection.MongoDBConnectionImplicits._

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Try

object ShortSaleVolumeDownloaderCmdLine extends QuantPMCmdLine{
  @CmdOption(name = "-to", required = false, usage = "yyyyMMdd", handler = classOf[LocalDateHandler])
  var toDate: LocalDate = LocalDate.now()
  @CmdOption(name = "-from", required = false, usage = "yyyyMMdd", handler = classOf[LocalDateOptionHandler])
  var fromDate: Option[LocalDate] = None
  @CmdOption(name = "-threadNum", required = false, usage = "Integer")
  var threadNum: Int = 1
}

object ShortSaleVolumeDownloader extends QuantPMApp(ShortSaleVolumeDownloaderCmdLine) with Log{
  val toDate = cmdLine.toDate
  val fromDate = cmdLine.fromDate.getOrElse(cmdLine.toDate)

  execute(fromDate, toDate)

  def execute(fromDate: LocalDate, toDate: LocalDate): Unit = {
    require(!fromDate.isAfter(toDate), "start date can not later than end date")
    val dates = {
      def collectDates(from: LocalDate, to: LocalDate, acc: Vector[LocalDate]): Vector[LocalDate] =
        if (from == to) acc :+ to else collectDates(from.plusDays(1), to, acc :+ from)
      collectDates(fromDate, toDate, Vector())
    }
    val permutation = dates.cross(ShortSaleVolume.sourceList.values).par
    permutation.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(cmdLine.threadNum))

    permutation.par.foreach{
      case (date, exchange) =>
        if (RegShoRecord.countInDateAndMarket(date.asDate, exchange)>0){
          log.warn(s"Data for $exchange at $date already exists")
        } else {
          val (statusCode, records) = ShortSaleVolume.getData(exchange, date)
          if (statusCode < 300) {
            log.info(s"Download data from $exchange for $date successfully")
            val results = Try{if (records.nonEmpty) RegShoRecord.putMany(records).results else Seq()}.toEither
            results match {
              case Left(e) =>
                log.error(s"Insert data from $exchange for $date to database failed")
                e.getStackTrace.foreach(trace => log.error(trace.toString))
              case Right(result) => log.info(result.toString)
            }
          } else {
            log.error(s"Download data from $exchange for $date failed with $statusCode")
          }
        }
    }
  }
}