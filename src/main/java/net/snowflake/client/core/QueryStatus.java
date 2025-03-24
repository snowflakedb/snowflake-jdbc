package net.snowflake.client.core;

/** Copied from QueryDTO.java in Global Services. */
public enum QueryStatus {
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
  private String errorMessage = "No error reported";
  private int errorCode = 0;

  QueryStatus(int value, String description) {
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
   * @deprecated use {@link net.snowflake.client.jdbc.QueryStatusV2} instead
   * @return error message
   */
  @Deprecated
  public String getErrorMessage() {
    return this.errorMessage;
  }

  /**
   * @deprecated use {@link net.snowflake.client.jdbc.QueryStatusV2} instead
   * @return error code
   */
  @Deprecated
  public int getErrorCode() {
    return this.errorCode;
  }

  /**
   * @deprecated use {@link net.snowflake.client.jdbc.QueryStatusV2} instead
   * @param message the error message
   */
  @Deprecated
  public void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  /**
   * @deprecated use {@link net.snowflake.client.jdbc.QueryStatusV2} instead
   * @param errorCode the error code
   */
  @Deprecated
  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  /**
   * Check if query is still running.
   *
   * @param status QueryStatus
   * @return true if query is still running
   */
  public static boolean isStillRunning(QueryStatus status) {
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
  public static boolean isAnError(QueryStatus status) {
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
  public static QueryStatus getStatusFromString(String description) {
    if (description != null) {
      for (QueryStatus st : QueryStatus.values()) {
        if (description.equalsIgnoreCase(st.getDescription())) {
          return st;
        }
      }
      return QueryStatus.NO_DATA;
    }
    return null;
  }
}
