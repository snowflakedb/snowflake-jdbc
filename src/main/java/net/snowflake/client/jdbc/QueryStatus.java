package net.snowflake.client.jdbc;

public final class QueryStatus {
  private final long endTime;
  private final int errorCode;
  private final String errorMessage;
  private final String id;
  private final String name;
  private final long sessionId;
  private final String sqlText;
  private final long startTime;
  private final String state;
  private final Status status;
  private final int totalDuration;
  private final String warehouseExternalSize;
  private final int warehouseId;
  private final String warehouseName;
  private final String warehouseServerType;

  public QueryStatus(
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
    this.status = Status.getStatusFromString(name);
    this.totalDuration = totalDuration;
    this.warehouseExternalSize = warehouseExternalSize;
    this.warehouseId = warehouseId;
    this.warehouseName = warehouseName;
    this.warehouseServerType = warehouseServerType;
  }

  public static QueryStatus empty() {
    return new QueryStatus(0, 0, "", "", "", 0, "", 0, "", 0, "", 0, "", "");
  }

  public boolean isEmpty() {
    return name.isEmpty();
  }

  public boolean isStillRunning() {
    return Status.isStillRunning(status);
  }

  public boolean isSuccess() {
    return status == Status.SUCCESS;
  }

  public boolean isAnError() {
    return Status.isAnError(status);
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

  /**
   * To preserve compatibility with {@link Status}
   *
   * @return name
   */
  public String getDescription() {
    return name;
  }

  public Status getStatus() {
    return status;
  }

  public enum Status {
    RUNNING(0, "RUNNING"),
    ABORTING(1, "ABORTING"),
    SUCCESS(2, "SUCCESS"),
    FAILED_WITH_ERROR(3, "FAILED_WITH_ERROR"),
    ABORTED(4, "ABORTED"),
    QUEUED(5, "QUEUED"),
    FAILED_WITH_INCIDENT(6, "FAILED_WITH_INCIDENT"),
    DISCONNECTED(7, "DISCONNECTED"),
    RESUMING_WAREHOUSE(8, "RESUMING_WAREHOUSE"),
    // purposeful typo. Is present in QueryDTO.java.
    QUEUED_REPAIRING_WAREHOUSE(9, "QUEUED_REPARING_WAREHOUSE"),
    RESTARTED(10, "RESTARTED"),

    /** The state when a statement is waiting on a lock on resource held by another statement. */
    BLOCKED(11, "BLOCKED"),
    NO_DATA(12, "NO_DATA");

    private final int value;
    private final String description;

    Status(int value, String description) {
      this.value = value;
      this.description = description;
    }

    public int getValue() {
      return this.value;
    }

    public String getDescription() {
      return this.description;
    }

    /**
     * Check if query is still running.
     *
     * @param status QueryStatus
     * @return true if query is still running
     */
    public static boolean isStillRunning(QueryStatus.Status status) {
      switch (status.getValue()) {
        case 0: // "RUNNING"
        case 5: // "QUEUED"
        case 8: // "RESUMING_WAREHOUSE"
        case 9: // "QUEUED_REPAIRING_WAREHOUSE"
        case 11: // "BLOCKED"
        case 12: // "NO_DATA"
          return true;
        default:
          return false;
      }
    }

    /**
     * Check if query status is an error
     *
     * @param status QueryStatus
     * @return true if query status is an error status
     */
    public static boolean isAnError(QueryStatus.Status status) {
      switch (status.getValue()) {
        case 1: // Aborting
        case 3: // Failed with error
        case 4: // Aborted
        case 6: // Failed with incident
        case 7: // disconnected
        case 11: // blocked
          return true;
        default:
          return false;
      }
    }

    /**
     * Get the query status from a string description
     *
     * @param description the status description
     * @return QueryStatus
     */
    public static QueryStatus.Status getStatusFromString(String description) {
      if (description != null) {
        for (QueryStatus.Status st : QueryStatus.Status.values()) {
          if (description.equalsIgnoreCase(st.getDescription())) {
            return st;
          }
        }
        return QueryStatus.Status.NO_DATA;
      }
      return null;
    }
  }
}
