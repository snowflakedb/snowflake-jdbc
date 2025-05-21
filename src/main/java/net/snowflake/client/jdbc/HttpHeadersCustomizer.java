package net.snowflake.client.jdbc;

import java.util.List;
import java.util.Map;

/**
 * Allows programmatic customization of HTTP headers for requests sent by the Snowflake JDBC driver.
 *
 * <p>Implementations can be registered with the driver (e.g., via {@code SnowflakeBasicDataSource})
 * to dynamically add headers. They define which requests to apply headers to, provide the headers,
 * and specify if headers should regenerate on retries (e.g., for dynamic tokens).
 */
public interface HttpHeadersCustomizer {
  public static final String HTTP_HEADER_CUSTOMIZERS_PROPERTY_KEY =
      "net.snowflake.client.jdbc.HttpHeadersCustomizer";

  /**
   * Determines if this customizer should be applied to the given request context.
   *
   * @param method The HTTP method (e.g., "GET", "POST").
   * @param uri The target URI for the request.
   * @param currentHeaders A read-only view of headers already present before this customizer runs.
   * @return true if newHeaders() should be called for this request, false otherwise.
   */
  boolean applies(String method, String uri, Map<String, List<String>> currentHeaders);

  /**
   * Generates the custom headers to be added to the request.
   *
   * @return A Map where keys are header names and values are Lists of header values.
   */
  Map<String, List<String>> newHeaders();

  /**
   * Indicates if newHeaders() should be called only once for the initial request attempt (true), or
   * if it should be called again before each retry attempt (false).
   *
   * @return true for static headers, false for dynamic headers needing regeneration.
   */
  boolean invokeOnce();
}
