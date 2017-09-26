package qpm.data.acquire

import org.apache.http.message.BasicHttpResponse

object HttpClientUtilTest extends App {
  val res = HttpClientUtil.getWebPage("http://www.google.com")
  println(res)

}
