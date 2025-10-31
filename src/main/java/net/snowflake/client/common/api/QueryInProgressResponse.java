package net.snowflake.client.common.api;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Response object returned to the client if the query has not completed before the client
 * health-check interval. It serves multiple purposes:
 *
 * <p>(1) provides incremental progress report to the client,
 *
 * <p>(2) ensures that the client is alive and waiting for the result while we are executing the
 * query,
 *
 * <p>(3) ensures that we do not hold the HTTP connection open and idle for long periods of time.
 *
 * <p>
 *
 * @author ppovinec
 */
@SnowflakeJdbcInternalApi
public class QueryInProgressResponse {
  // Code that Snowflake server uses as query-in-progress indicator.
  public static final String QUERY_IN_PROGRESS_CODE = "333333";
  public static final String QUERY_IN_PROGRESS_ASYNC_CODE = "333334";

  // Globally unique query identifier.
  private String queryId;

  // URL that the client is expected to access to obtain the query result.
  private String getResultUrl;

  // Tolerance time in seconds that the server waits for client to reconnect.
  private int queryAbortsAfterSecs;

  // Progress descriptor - TBD
  private Object progressDesc;

  public QueryInProgressResponse() {}

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getGetResultUrl() {
    return getResultUrl;
  }

  public void setGetResultUrl(String getResultUrl) {
    this.getResultUrl = getResultUrl;
  }

  public int getQueryAbortsAfterSecs() {
    return queryAbortsAfterSecs;
  }

  public void setQueryAbortsAfterSecs(int queryAbortsAfterSecs) {
    this.queryAbortsAfterSecs = queryAbortsAfterSecs;
  }

  public Object getProgressDesc() {
    return progressDesc;
  }

  public void setProgressDesc(Object progressDesc) {
    this.progressDesc = progressDesc;
  }
}
