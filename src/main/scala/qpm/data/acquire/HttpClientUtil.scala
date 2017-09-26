package qpm.data.acquire

import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.BufferedHttpEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.entity.ContentType
import java.io.InputStreamReader

import org.apache.commons.io.IOUtils


object HttpClientUtil {
  private val httpclient: CloseableHttpClient = HttpClients.createDefault

  case class WebPage private(statusCode: Int, content: String)

  def getWebPage(url: String): WebPage = {
    val httpGet = new HttpGet(url)
    val response = httpclient.execute(httpGet)
    val statusCode = response.getStatusLine.getStatusCode
    if (statusCode>=300) WebPage(statusCode, "") else {
      val entityRaw = response.getEntity
      val entity = if (entityRaw != null) new BufferedHttpEntity(entityRaw) else entityRaw
      val contentType = ContentType.getOrDefault(entity)
      val charset = contentType.getCharset
      WebPage(statusCode, IOUtils.toString(entity.getContent, charset))
    }
  }
}
