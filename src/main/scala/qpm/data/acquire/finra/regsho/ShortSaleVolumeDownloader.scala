package qpm.data.acquire.finra.regsho

import java.time.LocalDate

import qpm.system._
import org.kohsuke.args4j.{Option => CmdOption}
import qpm.data.connection.MongoDBConnectionImplicits._

object ShortSaleVolumeDownloaderCmdLine extends QuantPMCmdLine{
  @CmdOption(name = "-to", required = false, usage = "yyyyMMdd", handler = classOf[LocalDateHandler])
  var toDate: LocalDate = LocalDate.now()
  @CmdOption(name = "-from", required = false, usage = "yyyyMMdd", handler = classOf[LocalDateOptionHandler])
  var fromDate: Option[LocalDate] = None
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
    val permutation = dates.cross(ShortSaleVolume.exchangeList)

    val insertResult = permutation.map{
      case (date, exchange) =>
        val (statusCode, records) = ShortSaleVolume.getData(exchange, date)
        if (statusCode < 300) {
          val results = if (records.nonEmpty) RegShoRecord.putMany(records).results else Seq()
          log.info(results.toString)
          (date, exchange, statusCode, results)
        } else {
          log.error(s"Download data from $exchange for $date faild with $statusCode")
          (date, exchange, statusCode, Vector())
        }
    }
  }
}