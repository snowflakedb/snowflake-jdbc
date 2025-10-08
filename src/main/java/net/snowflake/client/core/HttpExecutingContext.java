package net.snowflake.client.core;

import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.util.DecorrelatedJitterBackoff;

@SnowflakeJdbcInternalApi
public class HttpExecutingContext {

  // min backoff in milli before we retry due to transient issues
  private static final long minBackoffMillis = 1000;

  // max backoff in milli before we retry due to transient issues
  // we double the backoff after each retry till we reach the max backoff
  private static final long maxBackoffMillis = 16000;

  // retry at least once even if timeout limit has been reached
  private static final int MIN_RETRY_COUNT = 1;

  // retry at least once even if timeout limit has been reached
  private static final int DEFAULT_RETRY_TIMEOUT = 300;

  private final String requestId;
  private final String requestInfoScrubbed;
  private final long startTime;
  // start time for each request,
  // used for keeping track how much time we have spent
  // due to network issues so that we can compare against the user
  // specified network timeout to make sure we do not retry infinitely
  // when there are transient network/GS issues
  private long startTimePerRequest;
  // Used to indicate that this is a login/auth request and will be using the new retry strategy.
  private boolean isLoginRequest;
  //  Tracks the total time spent handling transient network issues and retries during HTTP requests
  private long elapsedMilliForTransientIssues;
  private long retryTimeout;
  private long authTimeout;
  private DecorrelatedJitterBackoff backoff;
  private long backoffInMillis;
  private int origSocketTimeout;
  private String breakRetryReason;
  private String breakRetryEventName;
  private String lastStatusCodeForRetry;
  private int retryCount;
  private int maxRetries;
  private boolean noRetry;
  private int injectSocketTimeout;
  private boolean retryHTTP403;
  private boolean shouldRetry;
  private boolean skipRetriesBecauseOf200; // todo create skip retry reason enum
  private boolean withoutCookies;
  private boolean includeRetryParameters;
  private boolean includeSnowflakeHeaders;
  private boolean unpackResponse;
  private AtomicBoolean canceling;
  private SFBaseSession sfSession;

  public HttpExecutingContext(String requestIdStr, String requestInfoScrubbed) {
    this.requestId = requestIdStr;
    this.requestInfoScrubbed = requestInfoScrubbed;
    this.startTime = System.currentTimeMillis();
    this.startTimePerRequest = startTime;
    this.backoff = new DecorrelatedJitterBackoff(getMinBackoffInMillis(), getMaxBackoffInMilli());
    this.backoffInMillis = minBackoffMillis;
  }

  public String getRequestId() {
    return requestId;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getStartTimePerRequest() {
    return startTimePerRequest;
  }

  public void setStartTimePerRequest(long startTimePerRequest) {
    this.startTimePerRequest = startTimePerRequest;
  }

  public boolean isLoginRequest() {
    return isLoginRequest;
  }

  public void setLoginRequest(boolean loginRequest) {
    isLoginRequest = loginRequest;
  }

  public long getElapsedMilliForTransientIssues() {
    return elapsedMilliForTransientIssues;
  }

  public long getRetryTimeoutInMilliseconds() {
    return retryTimeout * 1000;
  }

  public long getRetryTimeout() {
    return retryTimeout;
  }

  public void setRetryTimeout(long retryTimeout) {
    this.retryTimeout = retryTimeout;
  }

  public long getMinBackoffInMillis() {
    return minBackoffMillis;
  }

  public long getBackoffInMillis() {
    return backoffInMillis;
  }

  public void setBackoffInMillis(long backoffInMillis) {
    this.backoffInMillis = backoffInMillis;
  }

  public long getMaxBackoffInMilli() {
    return maxBackoffMillis;
  }

  public long getAuthTimeout() {
    return authTimeout;
  }

  public long getAuthTimeoutInMilliseconds() {
    return authTimeout * 1000;
  }

  public void setAuthTimeout(long authTimeout) {
    this.authTimeout = authTimeout;
  }

  public DecorrelatedJitterBackoff getBackoff() {
    return backoff;
  }

  public void setBackoff(DecorrelatedJitterBackoff backoff) {
    this.backoff = backoff;
  }

  public int getOrigSocketTimeout() {
    return origSocketTimeout;
  }

  public void setOrigSocketTimeout(int origSocketTimeout) {
    this.origSocketTimeout = origSocketTimeout;
  }

  public String getBreakRetryReason() {
    return breakRetryReason;
  }

  public void setBreakRetryReason(String breakRetryReason) {
    this.breakRetryReason = breakRetryReason;
  }

  public String getBreakRetryEventName() {
    return breakRetryEventName;
  }

  public void setBreakRetryEventName(String breakRetryEventName) {
    this.breakRetryEventName = breakRetryEventName;
  }

  public String getLastStatusCodeForRetry() {
    return lastStatusCodeForRetry;
  }

  public void setLastStatusCodeForRetry(String lastStatusCodeForRetry) {
    this.lastStatusCodeForRetry = lastStatusCodeForRetry;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(int retryCount) {
    this.retryCount = retryCount;
  }

  public void resetRetryCount() {
    this.retryCount = 0;
  }

  public void incrementRetryCount() {
    this.retryCount++;
  }

  public int getMaxRetries() {
    return maxRetries;
  }

  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public String getRequestInfoScrubbed() {
    return requestInfoScrubbed;
  }

  public boolean isNoRetry() {
    return noRetry;
  }

  public void setNoRetry(boolean noRetry) {
    this.noRetry = noRetry;
  }

  public boolean isRetryHTTP403() {
    return retryHTTP403;
  }

  public void setRetryHTTP403(boolean retryHTTP403) {
    this.retryHTTP403 = retryHTTP403;
  }

  public boolean isShouldRetry() {
    return shouldRetry;
  }

  public void setShouldRetry(boolean shouldRetry) {
    this.shouldRetry = shouldRetry;
  }

  public void increaseElapsedMilliForTransientIssues(long elapsedMilliForLastCall) {
    this.elapsedMilliForTransientIssues += elapsedMilliForLastCall;
  }

  public boolean elapsedTimeExceeded() {
    return elapsedMilliForTransientIssues > getRetryTimeoutInMilliseconds();
  }

  public boolean moreThanMinRetries() {
    return retryCount >= MIN_RETRY_COUNT;
  }

  public boolean maxRetriesExceeded() {
    return maxRetries > 0 && retryCount >= maxRetries;
  }

  public boolean socketOrConnectTimeoutReached() {
    return authTimeout > 0
        && elapsedMilliForTransientIssues > getAuthTimeoutInMilliseconds()
        && (origSocketTimeout == 0 || elapsedMilliForTransientIssues < origSocketTimeout);
  }

  public AtomicBoolean getCanceling() {
    return canceling;
  }

  public void setCanceling(AtomicBoolean canceling) {
    this.canceling = canceling;
  }

  public boolean isIncludeSnowflakeHeaders() {
    return includeSnowflakeHeaders;
  }

  public void setIncludeSnowflakeHeaders(boolean includeSnowflakeHeaders) {
    this.includeSnowflakeHeaders = includeSnowflakeHeaders;
  }

  public boolean isWithoutCookies() {
    return withoutCookies;
  }

  public void setWithoutCookies(boolean withoutCookies) {
    this.withoutCookies = withoutCookies;
  }

  public int isInjectSocketTimeout() {
    return injectSocketTimeout;
  }

  public void setInjectSocketTimeout(int injectSocketTimeout) {
    this.injectSocketTimeout = injectSocketTimeout;
  }

  public int getInjectSocketTimeout() {
    return injectSocketTimeout;
  }

  public boolean isIncludeRetryParameters() {
    return includeRetryParameters;
  }

  public boolean isUnpackResponse() {
    return unpackResponse;
  }

  public void setUnpackResponse(boolean unpackResponse) {
    this.unpackResponse = unpackResponse;
  }

  public void setIncludeRetryParameters(boolean includeRetryParameters) {
    this.includeRetryParameters = includeRetryParameters;
  }

  public boolean isSkipRetriesBecauseOf200() {
    return skipRetriesBecauseOf200;
  }

  public void setSkipRetriesBecauseOf200(boolean skipRetriesBecauseOf200) {
    this.skipRetriesBecauseOf200 = skipRetriesBecauseOf200;
  }

  public SFBaseSession getSfSession() {
    return sfSession;
  }

  public void setSfSession(SFBaseSession sfSession) {
    this.sfSession = sfSession;
  }
}
