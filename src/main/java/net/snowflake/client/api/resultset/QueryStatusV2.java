package net.snowflake.client.api.resultset;

/**
 * Represents detailed status information for a query execution.
 *
 * <p>This class provides comprehensive metadata about a query's execution, including timing
 * information, warehouse details, and error information if applicable. Use this class to monitor
 * query progress and diagnose execution issues.
 *
 * <h2 id="usage-example">Usage Example</h2>
 *
 * <pre>{@code
 * String queryId = statement.unwrap(SnowflakeStatement.class).getQueryID();
 * QueryStatusV2 status = connection.unwrap(SnowflakeConnection.class)
 *     .getQueryStatusV2(queryId);
 *
 * System.out.println("Query Status: " + status.getStatus());
 * System.out.println("Duration: " + status.getTotalDuration() + "ms");
 * if (status.getErrorCode() != 0) {
 *     System.err.println("Error: " + status.getErrorMessage());
 * }
 * }</pre>
 *
 * @see QueryStatus
 */
public final class QueryStatusV2 {
  private final long endTime;
  private final int errorCode;
  private final String errorMessage;
  private final String id;
  private final String name;
  private final long sessionId;
  private final String sqlText;
  private final long startTime;
  private final String state;
  private final QueryStatus status;
  private final int totalDuration;
  private final String warehouseExternalSize;
  private final int warehouseId;
  private final String warehouseName;
  private final String warehouseServerType;

  /**
   * Constructs a QueryStatusV2 object with detailed query execution information.
   *
   * @param endTime the end time of the query in milliseconds since epoch
   * @param errorCode the error code if query failed, 0 otherwise
   * @param errorMessage the error message if query failed, empty otherwise
   * @param id the unique query ID
   * @param name the query status name
   * @param sessionId the session ID that executed the query
   * @param sqlText the SQL text of the query
   * @param startTime the start time of the query in milliseconds since epoch
   * @param state the internal state of the query
   * @param totalDuration the total duration of query execution in milliseconds
   * @param warehouseExternalSize the external size of the warehouse (e.g., "X-Small")
   * @param warehouseId the warehouse ID
   * @param warehouseName the warehouse name
   * @param warehouseServerType the warehouse server type
   */
  public QueryStatusV2(
      long endTime,
      int errorCode,
      String errorMessage,
      String id,
      String name,
      long sessionId,
      String sqlText,
      long startTime,
      String state,
      int totalDuration,
      String warehouseExternalSize,
      int warehouseId,
      String warehouseName,
      String warehouseServerType) {
    this.endTime = endTime;
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
    this.id = id;
    this.name = name;
    this.sessionId = sessionId;
    this.sqlText = sqlText;
    this.startTime = startTime;
    this.state = state;
    this.status = QueryStatus.getStatusFromString(name);
    this.totalDuration = totalDuration;
    this.warehouseExternalSize = warehouseExternalSize;
    this.warehouseId = warehouseId;
    this.warehouseName = warehouseName;
    this.warehouseServerType = warehouseServerType;
  }

  /**
   * Creates an empty QueryStatusV2 instance with default values.
   *
   * @return an empty QueryStatusV2 object
   */
  public static QueryStatusV2 empty() {
    return new QueryStatusV2(0, 0, "", "", "", 0, "", 0, "", 0, "", 0, "", "");
  }

  /**
   * Checks if this query status is empty (no data).
   *
   * @return true if the status name is empty
   */
  public boolean isEmpty() {
    return name.isEmpty();
  }

  /**
   * Checks if the query is still running.
   *
   * @return true if the query is in a running state
   */
  public boolean isStillRunning() {
    return QueryStatus.isStillRunning(status);
  }

  /**
   * Checks if the query completed successfully.
   *
   * @return true if the query status is SUCCESS
   */
  public boolean isSuccess() {
    return status == QueryStatus.SUCCESS;
  }

  /**
   * Checks if the query encountered an error.
   *
   * @return true if the query is in an error state
   */
  public boolean isAnError() {
    return QueryStatus.isAnError(status);
  }

  /**
   * Gets the end time of query execution.
   *
   * @return the end time in milliseconds since epoch
   */
  public long getEndTime() {
    return endTime;
  }

  /**
   * Gets the error code if the query failed.
   *
   * @return the error code, or 0 if no error occurred
   */
  public int getErrorCode() {
    return errorCode;
  }

  /**
   * Gets the error message if the query failed.
   *
   * @return the error message, or empty string if no error occurred
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Gets the unique query ID.
   *
   * @return the query ID
   */
  public String getId() {
    return id;
  }

  /**
   * Gets the query status name.
   *
   * @return the status name (e.g., "RUNNING", "SUCCESS")
   */
  public String getName() {
    return name;
  }

  /**
   * Gets the session ID that executed the query.
   *
   * @return the session ID
   */
  public long getSessionId() {
    return sessionId;
  }

  /**
   * Gets the SQL text of the query.
   *
   * @return the SQL query text
   */
  public String getSqlText() {
    return sqlText;
  }

  /**
   * Gets the start time of query execution.
   *
   * @return the start time in milliseconds since epoch
   */
  public long getStartTime() {
    return startTime;
  }

  /**
   * Gets the internal state of the query.
   *
   * @return the internal query state
   */
  public String getState() {
    return state;
  }

  /**
   * Gets the total duration of query execution.
   *
   * @return the total duration in milliseconds
   */
  public int getTotalDuration() {
    return totalDuration;
  }

  /**
   * Gets the external size of the warehouse that executed the query.
   *
   * @return the warehouse size (e.g., "X-Small", "Small", "Medium")
   */
  public String getWarehouseExternalSize() {
    return warehouseExternalSize;
  }

  /**
   * Gets the warehouse ID that executed the query.
   *
   * @return the warehouse ID
   */
  public int getWarehouseId() {
    return warehouseId;
  }

  /**
   * Gets the warehouse name that executed the query.
   *
   * @return the warehouse name
   */
  public String getWarehouseName() {
    return warehouseName;
  }

  /**
   * Gets the warehouse server type that executed the query.
   *
   * @return the warehouse server type
   */
  public String getWarehouseServerType() {
    return warehouseServerType;
  }

  /**
   * Gets the status description (preserved for compatibility with {@link QueryStatus}).
   *
   * @return the status name
   */
  public String getDescription() {
    return name;
  }

  /**
   * Gets the query status enum value.
   *
   * @return the QueryStatus enum representing the current state
   */
  public QueryStatus getStatus() {
    return status;
  }
}
