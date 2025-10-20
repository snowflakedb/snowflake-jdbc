package net.snowflake.client.core;

import java.util.Collections;
import java.util.Map;

/**
 * Simple container for HTTP response data including both body and headers. This provides a clean
 * interface for methods that need access to response headers without exposing internal HTTP client
 * implementation details.
 */
@SnowflakeJdbcInternalApi
public class HttpResponseWithHeaders {
  private final String responseBody;
  private final Map<String, String> headers;

  public HttpResponseWithHeaders(String responseBody, Map<String, String> headers) {
    this.responseBody = responseBody;
    this.headers = headers != null ? Collections.unmodifiableMap(headers) : Collections.emptyMap();
  }

  /**
   * Get the HTTP response body as a string.
   *
   * @return the response body
   */
  public String getResponseBody() {
    return responseBody;
  }

  /**
   * Get the HTTP response headers as an immutable map. If multiple headers exist with the same
   * name, only the last value is included.
   *
   * @return immutable map of header name to header value
   */
  public Map<String, String> getHeaders() {
    return headers;
  }
}
