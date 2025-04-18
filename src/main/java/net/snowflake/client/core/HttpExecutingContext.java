package net.snowflake.client.core;

import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.util.DecorrelatedJitterBackoff;

@SnowflakeJdbcInternalApi
public class HttpExecutingContext {

  // min backoff in milli before we retry due to transient issues
  private static final long minBackoff = 1000;

  // max backoff in milli before we retry due to transient issues
  // we double the backoff after each retry till we reach the max backoff
  //    private static final long maxBackoff= 16000;
  private static final long maxBackoff = 16;

  // retry at least once even if timeout limit has been reached
  private static final int MIN_RETRY_COUNT = 1;

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
  boolean isLoginRequest;
  long elapsedMilliForTransientIssues;
  long retryTimeout;
  long authTimeout;
  DecorrelatedJitterBackoff backoff;
  long backoffInMillis;
  int origSocketTimeout;
  String breakRetryReason;
  String breakRetryEventNam;
  String lastStatusCodeForRetry;
  int retryCount;
  int maxRetries;
  boolean noRetry;
  int injectSocketTimeout;
  boolean retryHTTP403;
  boolean shouldRetry;
  boolean skipRetriesBecauseOf200; // todo create skip retry reason enum
  boolean withoutCookies;
  boolean includeRetryParameters;
  boolean includeRequestGuid;
  boolean unpackResponse;
  AtomicBoolean canceling;

  public HttpExecutingContext(String requestIdStr, String requestInfoScrubbed) {
    this.requestId = requestIdStr;
    this.requestInfoScrubbed = requestInfoScrubbed;
    this.startTime = System.currentTimeMillis();
    this.startTimePerRequest = startTime;
    this.backoff = new DecorrelatedJitterBackoff(getMinBackoffInMillis(), getMaxBackoffInMilli());
    ;
    this.backoffInMillis = minBackoff;
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

  public void setElapsedMilliForTransientIssues(long elapsedMilliForTransientIssues) {
    this.elapsedMilliForTransientIssues = elapsedMilliForTransientIssues;
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
    return minBackoff;
  }

  public long getBackoffInMillis() {
    return backoffInMillis;
  }

  public void setBackoffInMillis(long backoffInMillis) {
    this.backoffInMillis = backoffInMillis;
  }

  public long getMaxBackoffInMilli() {
    return maxBackoff;
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

  public String getBreakRetryEventNam() {
    return breakRetryEventNam;
  }

  public void setBreakRetryEventNam(String breakRetryEventNam) {
    this.breakRetryEventNam = breakRetryEventNam;
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

  public boolean isIncludeRequestGuid() {
    return includeRequestGuid;
  }

  public void setIncludeRequestGuid(boolean includeRequestGuid) {
    this.includeRequestGuid = includeRequestGuid;
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
}
