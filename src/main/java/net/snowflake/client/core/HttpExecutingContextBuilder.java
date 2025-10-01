package net.snowflake.client.core;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Builder class for {@link HttpExecutingContext}. Provides a fluent interface for constructing
 * HttpExecutingContext instances with many optional parameters.
 */
@SnowflakeJdbcInternalApi
public class HttpExecutingContextBuilder {
  private final String requestId;
  private final String requestInfoScrubbed;
  private long retryTimeout;
  private long authTimeout;
  private int origSocketTimeout;
  private int maxRetries;
  private int injectSocketTimeout;
  private AtomicBoolean canceling;
  private boolean withoutCookies;
  private boolean includeRetryParameters;
  private boolean includeSnowflakeHeaders;
  private boolean retryHTTP403;
  private boolean noRetry;
  private boolean unpackResponse;
  private boolean isLoginRequest;
  private SFBaseSession sfSession;

  /**
   * Creates a new builder instance with required parameters.
   *
   * @param requestId Request ID for logging and tracking
   * @param requestInfoScrubbed Scrubbed request info for logging
   */
  public HttpExecutingContextBuilder(String requestId, String requestInfoScrubbed) {
    this.requestId = requestId;
    this.requestInfoScrubbed = requestInfoScrubbed;
  }

  /**
   * Copy constructor to create a new builder from an existing HttpExecutingContext.
   *
   * @param context The context to copy settings from
   */
  public HttpExecutingContextBuilder(HttpExecutingContext context) {
    this.requestId = context.getRequestId();
    this.requestInfoScrubbed = context.getRequestInfoScrubbed();
    this.retryTimeout = context.getRetryTimeout();
    this.authTimeout = context.getAuthTimeout();
    this.origSocketTimeout = context.getOrigSocketTimeout();
    this.maxRetries = context.getMaxRetries();
    this.injectSocketTimeout = context.getInjectSocketTimeout();
    this.canceling = context.getCanceling();
    this.withoutCookies = context.isWithoutCookies();
    this.includeRetryParameters = context.isIncludeRetryParameters();
    this.includeSnowflakeHeaders = context.isIncludeSnowflakeHeaders();
    this.retryHTTP403 = context.isRetryHTTP403();
    this.noRetry = context.isNoRetry();
    this.unpackResponse = context.isUnpackResponse();
    this.isLoginRequest = context.isLoginRequest();
  }

  /**
   * Creates a new builder for a login request with common defaults.
   *
   * @param requestId Request ID for logging and tracking
   * @param requestInfoScrubbed Scrubbed request info for logging
   * @return A new builder instance configured for login requests
   */
  public static HttpExecutingContextBuilder forLogin(String requestId, String requestInfoScrubbed) {
    return new HttpExecutingContextBuilder(requestId, requestInfoScrubbed)
        .loginRequest(true)
        .includeSnowflakeHeaders(true)
        .retryHTTP403(true);
  }

  /**
   * Creates a new builder for a query request with common defaults.
   *
   * @param requestId Request ID for logging and tracking
   * @param requestInfoScrubbed Scrubbed request info for logging
   * @return A new builder instance configured for query requests
   */
  public static HttpExecutingContextBuilder forQuery(String requestId, String requestInfoScrubbed) {
    return new HttpExecutingContextBuilder(requestId, requestInfoScrubbed)
        .includeRetryParameters(true)
        .includeSnowflakeHeaders(true)
        .unpackResponse(true);
  }

  /**
   * Creates a new builder for a simple HTTP request with minimal retry settings.
   *
   * @param requestId Request ID for logging and tracking
   * @param requestInfoScrubbed Scrubbed request info for logging
   * @return A new builder instance configured for simple requests
   */
  public static HttpExecutingContextBuilder forSimpleRequest(
      String requestId, String requestInfoScrubbed) {
    return new HttpExecutingContextBuilder(requestId, requestInfoScrubbed)
        .noRetry(true)
        .includeSnowflakeHeaders(true);
  }

  /**
   * Creates a new builder with default settings for retryable requests.
   *
   * @param requestId Request ID for logging and tracking
   * @param requestInfoScrubbed Scrubbed request info for logging
   * @return A new builder instance with default retry settings
   */
  public static HttpExecutingContextBuilder withRequest(
      String requestId, String requestInfoScrubbed) {
    return new HttpExecutingContextBuilder(requestId, requestInfoScrubbed);
  }

  /**
   * Sets the retry timeout in seconds.
   *
   * @param retryTimeout Retry timeout in seconds
   * @return this builder instance
   */
  public HttpExecutingContextBuilder retryTimeout(long retryTimeout) {
    this.retryTimeout = retryTimeout;
    return this;
  }

  /**
   * Sets the authentication timeout in seconds.
   *
   * @param authTimeout Authentication timeout in seconds
   * @return this builder instance
   */
  public HttpExecutingContextBuilder authTimeout(long authTimeout) {
    this.authTimeout = authTimeout;
    return this;
  }

  /**
   * Sets the original socket timeout in milliseconds.
   *
   * @param origSocketTimeout Socket timeout in milliseconds
   * @return this builder instance
   */
  public HttpExecutingContextBuilder origSocketTimeout(int origSocketTimeout) {
    this.origSocketTimeout = origSocketTimeout;
    return this;
  }

  /**
   * Sets the maximum number of retries.
   *
   * @param maxRetries Maximum number of retries
   * @return this builder instance
   */
  public HttpExecutingContextBuilder maxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * Sets the injected socket timeout for testing.
   *
   * @param injectSocketTimeout Socket timeout to inject
   * @return this builder instance
   */
  public HttpExecutingContextBuilder injectSocketTimeout(int injectSocketTimeout) {
    this.injectSocketTimeout = injectSocketTimeout;
    return this;
  }

  /**
   * Sets the canceling flag.
   *
   * @param canceling AtomicBoolean for cancellation
   * @return this builder instance
   */
  public HttpExecutingContextBuilder canceling(AtomicBoolean canceling) {
    this.canceling = canceling;
    return this;
  }

  /**
   * Sets whether to disable cookies.
   *
   * @param withoutCookies true to disable cookies
   * @return this builder instance
   */
  public HttpExecutingContextBuilder withoutCookies(boolean withoutCookies) {
    this.withoutCookies = withoutCookies;
    return this;
  }

  /**
   * Sets whether to include retry parameters in requests.
   *
   * @param includeRetryParameters true to include retry parameters
   * @return this builder instance
   */
  public HttpExecutingContextBuilder includeRetryParameters(boolean includeRetryParameters) {
    this.includeRetryParameters = includeRetryParameters;
    return this;
  }

  /**
   * Sets whether to include request GUID.
   *
   * @param includeSnowflakeHeaders true to include request GUID and other Snowflake headers
   * @return this builder instance
   */
  public HttpExecutingContextBuilder includeSnowflakeHeaders(boolean includeSnowflakeHeaders) {
    this.includeSnowflakeHeaders = includeSnowflakeHeaders;
    return this;
  }

  /**
   * Sets whether to retry on HTTP 403 errors.
   *
   * @param retryHTTP403 true to retry on HTTP 403
   * @return this builder instance
   */
  public HttpExecutingContextBuilder retryHTTP403(boolean retryHTTP403) {
    this.retryHTTP403 = retryHTTP403;
    return this;
  }

  /**
   * Sets whether to disable retries.
   *
   * @param noRetry true to disable retries
   * @return this builder instance
   */
  public HttpExecutingContextBuilder noRetry(boolean noRetry) {
    this.noRetry = noRetry;
    return this;
  }

  /**
   * Sets whether to unpack the response.
   *
   * @param unpackResponse true to unpack response
   * @return this builder instance
   */
  public HttpExecutingContextBuilder unpackResponse(boolean unpackResponse) {
    this.unpackResponse = unpackResponse;
    return this;
  }

  /**
   * Sets whether this is a login request.
   *
   * @param isLoginRequest true if this is a login request
   * @return this builder instance
   */
  public HttpExecutingContextBuilder loginRequest(boolean isLoginRequest) {
    this.isLoginRequest = isLoginRequest;
    return this;
  }

  /**
   * Sets the session associated with this context.
   *
   * @param sfSession SFBaseSession to associate with this context
   * @return this builder instance
   */
  public HttpExecutingContextBuilder withSfSession(SFBaseSession sfSession) {
    this.sfSession = sfSession;
    return this;
  }

  /**
   * Builds and returns a new HttpExecutingContext instance with the configured parameters.
   *
   * @return A new HttpExecutingContext instance
   */
  public HttpExecutingContext build() {
    HttpExecutingContext context = new HttpExecutingContext(requestId, requestInfoScrubbed);
    context.setRetryTimeout(retryTimeout);
    context.setAuthTimeout(authTimeout);
    context.setOrigSocketTimeout(origSocketTimeout);
    context.setMaxRetries(maxRetries);
    context.setInjectSocketTimeout(injectSocketTimeout);
    context.setCanceling(canceling);
    context.setWithoutCookies(withoutCookies);
    context.setIncludeRetryParameters(includeRetryParameters);
    context.setIncludeSnowflakeHeaders(includeSnowflakeHeaders);
    context.setRetryHTTP403(retryHTTP403);
    context.setNoRetry(noRetry);
    context.setUnpackResponse(unpackResponse);
    context.setLoginRequest(isLoginRequest);
    context.setSfSession(sfSession);
    return context;
  }
}
