package net.snowflake.client.jdbc;

import net.snowflake.client.core.QueryStatus;

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

  public static QueryStatusV2 empty() {
    return new QueryStatusV2(0, 0, "", "", "", 0, "", 0, "", 0, "", 0, "", "");
  }

  public boolean isEmpty() {
    return name.isEmpty();
  }

  public boolean isStillRunning() {
    return QueryStatus.isStillRunning(status);
  }

  public boolean isSuccess() {
    return status == QueryStatus.SUCCESS;
  }

  public boolean isAnError() {
    return QueryStatus.isAnError(status);
  }

  public long getEndTime() {
    return endTime;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public long getSessionId() {
    return sessionId;
  }

  public String getSqlText() {
    return sqlText;
  }

  public long getStartTime() {
    return startTime;
  }

  public String getState() {
    return state;
  }

  public int getTotalDuration() {
    return totalDuration;
  }

  public String getWarehouseExternalSize() {
    return warehouseExternalSize;
  }

  public int getWarehouseId() {
    return warehouseId;
  }

  public String getWarehouseName() {
    return warehouseName;
  }

  public String getWarehouseServerType() {
    return warehouseServerType;
  }

  /** To preserve compatibility with {@link QueryStatus} */
  public String getDescription() {
    return name;
  }

  public QueryStatus getStatus() {
    return status;
  }
}
