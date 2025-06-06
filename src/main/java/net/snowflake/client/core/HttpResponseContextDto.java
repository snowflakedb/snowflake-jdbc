package net.snowflake.client.core;

import org.apache.http.client.methods.CloseableHttpResponse;

@SnowflakeJdbcInternalApi
public class HttpResponseContextDto {

  private CloseableHttpResponse httpResponse;
  private String unpackedCloseableHttpResponse;
  private Exception savedEx;

  // Constructors
  public HttpResponseContextDto() {}

  public HttpResponseContextDto(
      CloseableHttpResponse httpResponse, String unpackedCloseableHttpResponse) {
    this.httpResponse = httpResponse;
    this.unpackedCloseableHttpResponse = unpackedCloseableHttpResponse;
  }

  public CloseableHttpResponse getHttpResponse() {
    return httpResponse;
  }

  public void setHttpResponse(CloseableHttpResponse httpResponse) {
    this.httpResponse = httpResponse;
  }

  public String getUnpackedCloseableHttpResponse() {
    return unpackedCloseableHttpResponse;
  }

  public void setUnpackedCloseableHttpResponse(String unpackedCloseableHttpResponse) {
    this.unpackedCloseableHttpResponse = unpackedCloseableHttpResponse;
  }

  public Exception getSavedEx() {
    return savedEx;
  }

  public void setSavedEx(Exception savedEx) {
    this.savedEx = savedEx;
  }

  @Override
  public String toString() {
    return "CloseableHttpResponseContextDto{"
        + "httpResponse="
        + httpResponse
        + ", unpackedCloseableHttpResponse="
        + unpackedCloseableHttpResponse
        + '}';
  }
}
