package qpm.data.acquire

object HttpClientUtilTest extends App {
  val httpget = HttpClientUtil.getWebPage("www.google.com")
  println(httpget)
}
