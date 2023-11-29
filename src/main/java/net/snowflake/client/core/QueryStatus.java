package net.snowflake.client.core;

import java.util.Arrays;
import java.util.List;

/** Copied from QueryDTO.java in Global Services. */
public class QueryStatus {
  public static final QueryStatus RUNNING = new QueryStatus(0, "RUNNING");
  public static final QueryStatus ABORTING = new QueryStatus(1, "ABORTING");
  public static final QueryStatus SUCCESS = new QueryStatus(2, "SUCCESS");
  public static final QueryStatus FAILED_WITH_ERROR = new QueryStatus(3, "FAILED_WITH_ERROR");
  public static final QueryStatus ABORTED = new QueryStatus(4, "ABORTED");
  public static final QueryStatus QUEUED = new QueryStatus(5, "QUEUED");
  public static final QueryStatus FAILED_WITH_INCIDENT = new QueryStatus(6, "FAILED_WITH_INCIDENT");
  public static final QueryStatus DISCONNECTED = new QueryStatus(7, "DISCONNECTED");
  public static final QueryStatus RESUMING_WAREHOUSE = new QueryStatus(8, "RESUMING_WAREHOUSE");
  // purposeful typo. Is present in QueryDTO.java.
  public static final QueryStatus QUEUED_REPAIRING_WAREHOUSE =
      new QueryStatus(9, "QUEUED_REPARING_WAREHOUSE");
  public static final QueryStatus RESTARTED = new QueryStatus(10, "RESTARTED");

  /** The state when a statement is waiting on a lock on resource held by another statement. */
  public static final QueryStatus BLOCKED = new QueryStatus(11, "BLOCKED");

  public static final QueryStatus NO_DATA = new QueryStatus(12, "NO_DATA");

  private static final List<QueryStatus> allStatuses =
      Arrays.asList(
          RUNNING,
          ABORTING,
          SUCCESS,
          FAILED_WITH_ERROR,
          ABORTED,
          QUEUED,
          FAILED_WITH_INCIDENT,
          DISCONNECTED,
          RESUMING_WAREHOUSE,
          QUEUED_REPAIRING_WAREHOUSE,
          RESTARTED,
          BLOCKED,
          NO_DATA);

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

  public String getErrorMessage() {
    return this.errorMessage;
  }

  public int getErrorCode() {
    return this.errorCode;
  }

  public void setErrorMessage(String message) {
    this.errorMessage = message;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

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

  public static QueryStatus getStatusFromString(String description) {
    if (description != null) {
      return allStatuses.stream()
          .filter(queryStatus -> queryStatus.description.equals(description))
          .findFirst()
          .map(queryStatus -> new QueryStatus(queryStatus.value, queryStatus.description))
          .orElse(new QueryStatus(NO_DATA.value, NO_DATA.description));
    }
    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof QueryStatus)) {
      return false;
    }
    QueryStatus another = (QueryStatus) obj;
    return value == another.value && description.equals(another.description);
  }

  @Override
  public int hashCode() {
    return value;
  }
}
