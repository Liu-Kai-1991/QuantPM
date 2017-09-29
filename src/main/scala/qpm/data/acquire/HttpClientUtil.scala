package qpm.data.acquire

import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.BufferedHttpEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.entity.ContentType
import org.apache.commons.io.IOUtils
import org.apache.http.client.config.RequestConfig
import qpm.system.Log

import scala.util.{Random, Try}


object HttpClientUtil extends Log{

  val timeOutErrorCode = 408
  val timeout = 5000
  val config: RequestConfig = RequestConfig.custom.setConnectTimeout(timeout)
    .setConnectionRequestTimeout(timeout).setCookieSpec(Random.nextInt.toString)
    .setSocketTimeout(timeout).build
  private def newHttpclient: CloseableHttpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()

  case class WebPage private(statusCode: Int, content: String){
    def good: Boolean = statusCode<300
    def bad: Boolean = !good: Boolean
  }

  def getWebPage(url: String): WebPage = {
    val httpGet = new HttpGet(url)
    val httpclient = newHttpclient
    val response = Try{httpclient.execute(httpGet)}.toOption
    val statusCode = response.map(_.getStatusLine.getStatusCode).getOrElse(timeOutErrorCode)
    if (statusCode>=300) {
      log.error(s"Download page $url faild with $statusCode")
      httpclient.close()
      WebPage(statusCode, "")
    } else {
      val entityRaw = response.get.getEntity
      val entity = if (entityRaw != null) new BufferedHttpEntity(entityRaw) else entityRaw
      val contentType = ContentType.getOrDefault(entity)
      val charset = contentType.getCharset
      val content = IOUtils.toString(entity.getContent, charset)
      httpclient.close()
      WebPage(statusCode, content)
    }
  }
}
