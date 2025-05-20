package net.snowflake.client.core;

import com.amazonaws.Request;
import com.amazonaws.handlers.RequestHandler2;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.client.jdbc.HttpHeadersCustomizer;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

/**
 * Implements Apache HttpClient's {@link HttpRequestInterceptor} to provide a mechanism for adding
 * custom HTTP headers to outgoing requests made by the Snowflake JDBC driver.
 *
 * <p>This class iterates through a list of user-provided {@link HttpHeadersCustomizer}
 * implementations. For each customizer, it checks if it applies to the current request. If it does,
 * it retrieves new headers from the customizer and adds them to the request, ensuring that existing
 * driver-set headers are not overridden.
 *
 * <p>For Apache HttpClient, retry detection is handled by checking the {@link
 * AttributeEnhancingHttpRequestRetryHandler#EXECUTION_COUNT_ATTRIBUTE} attribute in the {@link
 * HttpContext} set by {@link AttributeEnhancingHttpRequestRetryHandler} to honor the {@code
 * invokeOnce()} contract of the customizer.
 *
 * @see HttpHeadersCustomizer
 */
public class HeaderCustomizerHttpRequestInterceptor extends RequestHandler2
    implements HttpRequestInterceptor {
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(HeaderCustomizerHttpRequestInterceptor.class);
  private final List<HttpHeadersCustomizer> headersCustomizers;

  public HeaderCustomizerHttpRequestInterceptor(List<HttpHeadersCustomizer> headersCustomizers) {
    if (headersCustomizers != null) {
      this.headersCustomizers = new ArrayList<>(headersCustomizers); // Defensive copy
    } else {
      this.headersCustomizers = new ArrayList<>();
    }
  }

  /**
   * Processes an Apache HttpClient {@link HttpRequest} before it is sent. It iterates through
   * registered {@link HttpHeadersCustomizer}s, checks applicability, retrieves new headers,
   * verifies against overriding driver headers, and adds them to the request. Handles the {@code
   * invokeOnce()} flag based on the "execution-count" attribute in the {@link HttpContext}.
   *
   * @param httpRequest The HTTP request to process.
   * @param httpContext The context for the HTTP request execution, used to retrieve retry count.
   * @throws DriverHeaderOverridingNotAllowedException If a customizer attempts to override a
   *     driver-set header.
   */
  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    if (this.headersCustomizers.isEmpty()) {
      return;
    }
    String httpMethod = httpRequest.getRequestLine().getMethod();
    String uri = httpRequest.getRequestLine().getUri();
    Map<String, List<String>> currentHeaders = extractHeaders(httpRequest);
    // convert header names to lower case for case in-sensitive lookup
    Set<String> protectedHeaders =
        currentHeaders.keySet().stream().map(String::toLowerCase).collect(Collectors.toSet());
    Integer executionCount =
        (Integer)
            httpContext.getAttribute(
                AttributeEnhancingHttpRequestRetryHandler.EXECUTION_COUNT_ATTRIBUTE);
    boolean isRetry = (executionCount == null || executionCount > 0);

    for (HttpHeadersCustomizer customizer : this.headersCustomizers) {
      if (customizer.applies(httpMethod, uri, currentHeaders)) {
        if (customizer.invokeOnce() && isRetry) {
          logger.debug(
              "{} customizer should only run on the first attempt and this is a {} retry. Skipping.",
              customizer.getClass().getCanonicalName(),
              executionCount);
          continue;
        }
        Map<String, List<String>> newHeaders = customizer.newHeaders();

        throwIfExistingHeadersAreModified(protectedHeaders, newHeaders.keySet(), customizer);

        for (Map.Entry<String, List<String>> entry : newHeaders.entrySet()) {
          for (String value : entry.getValue()) {
            httpRequest.addHeader(entry.getKey(), value);
          }
        }
      }
    }
  }

  @Override
  public void beforeRequest(Request<?> request) {
    super.beforeRequest(request);
    if (this.headersCustomizers.isEmpty()) {
      return;
    }
    String httpMethod = request.getHttpMethod().name();
    String uri = request.getEndpoint().toString();
    Map<String, List<String>> currentHeaders = extractHeaders(request);
    Set<String> protectedHeaders =
        currentHeaders.keySet().stream()
            .map(String::toLowerCase)
            .collect(Collectors.toSet()); // convert to lower case for case in-sensitive lookup

    for (HttpHeadersCustomizer customizer : this.headersCustomizers) {
      if (customizer.applies(httpMethod, uri, currentHeaders)) {
        Map<String, List<String>> newHeaders = customizer.newHeaders();

        throwIfExistingHeadersAreModified(protectedHeaders, newHeaders.keySet(), customizer);

        logger.debug(
            "Customizer {} is adding headers {}",
            customizer.getClass().getCanonicalName(),
            newHeaders.keySet());
        for (Map.Entry<String, List<String>> entry : newHeaders.entrySet()) {
          for (String value : entry.getValue()) {
            request.addHeader(entry.getKey(), value);
          }
        }
      }
    }
  }

  private static Map<String, List<String>> extractHeaders(HttpRequest request) {
    Map<String, List<String>> headerMap = new HashMap<>();
    for (Header header : request.getAllHeaders()) {
      headerMap.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
    }
    return headerMap;
  }

  private static Map<String, List<String>> extractHeaders(Request<?> request) {
    Map<String, List<String>> headerMap = new HashMap<>();
    for (Map.Entry<String, String> entry : request.getHeaders().entrySet()) {
      headerMap.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(entry.getValue());
    }
    return headerMap;
  }

  /**
   * Checks if any header names from the customizer's new headers attempt to override existing
   * driver-set headers. Compares header names case-insensitively.
   *
   * @param protectedHeaders A set of lowercase header names initially present on the request.
   * @param newHeaders A set of header names provided by the customizer.
   * @param customizer The customizer attempting to add headers, for logging/exception messages.
   * @throws DriverHeaderOverridingNotAllowedException If an override is detected.
   */
  private static void throwIfExistingHeadersAreModified(
      Set<String> protectedHeaders, Set<String> newHeaders, HttpHeadersCustomizer customizer) {
    for (String headerName : newHeaders) {
      if (headerName != null && protectedHeaders.contains(headerName.toLowerCase())) {
        String customizerName = customizer.getClass().getCanonicalName();
        logger.debug(
            "Customizer {} attempted to override existing driver header: {}",
            customizerName,
            headerName);
        throw new DriverHeaderOverridingNotAllowedException(headerName, customizerName);
      }
    }
  }

  public static class DriverHeaderOverridingNotAllowedException extends RuntimeException {
    public DriverHeaderOverridingNotAllowedException(String header, String customizerName) {
      super(
          String.format(
              "Driver headers overriding not allowed. Tried for header: %s in customizer: %s",
              header, customizerName));
    }
  }
}
