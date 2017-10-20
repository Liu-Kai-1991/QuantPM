package qpm.data.acquire

import qpm.data.connection.HttpClientUtil
import qpm.system.Log

object HttpClientUtilTest extends App with Log {
  Log.setLogLevel(Log.DEBUG)
  log.info("info")
  log.debug("debug")
  log.warn("warn")
  val res = HttpClientUtil.getWebPage("http://www.google.com")
  println(res)
}
