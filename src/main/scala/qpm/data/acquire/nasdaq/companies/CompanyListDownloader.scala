package qpm.data.acquire.nasdaq.companies

import qpm.data.connection.MongoDBConnectionImplicits._
import qpm.system._

import scala.util.Try

object CompanyListDownloader extends App with Log{
  execute()

  def execute(): Unit = {
    val (statusCode, nasdaqCompanyInfo) = CompanyList.getData(CompanyList.byRegion)
    if (statusCode<300) {
      println(nasdaqCompanyInfo.get)

      val results = Try{NasdaqCompanyInfo.put(nasdaqCompanyInfo.get).results}.toEither
      results match {
        case Left(e) =>
          log.error(s"Insert data for ${nasdaqCompanyInfo.get.timeStamp} to database failed")
          e.getStackTrace.foreach(trace => log.error(trace.toString))
        case Right(result) => log.info(result.toString)
      }
    }
  }
}
