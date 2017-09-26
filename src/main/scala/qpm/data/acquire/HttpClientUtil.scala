package qpm.data.acquire

import org.apache.http.client.methods.HttpGet

object HttpClientUtil {
  def getWebPage(url: String) = new HttpGet(url)
}
