package net.snowflake.client.core;

/**
 * Copied from QueryDTO.java in Global Services.
 */
public enum QueryStatus
{
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

  /**
   * The state when a statement is waiting on a lock on resource held by another statement.
   */
  BLOCKED(11, "BLOCKED"),
  NO_DATA(12, "NO_DATA");

  private final int value;
  private final String description;

  QueryStatus(int value, String description)
  {
    this.value = value;
    this.description = description;
  }

  public int getValue()
  {
    return this.value;
  }

  public String getDescription()
  {
    return this.description;
  }

  public static boolean isStillRunning(QueryStatus status)
  {
    switch (status.getValue())
    {
      case 0: // "RUNNING"
      case 8: //"RESUMING_WAREHOUSE"
      case 5: //"QUEUED"
      case 9: //"QUEUED_REPAIRING_WAREHOUSE"
      case 12: //"NO_DATA"
        return true;
      default:
        return false;
    }
  }

  public static QueryStatus getStatusFromString(String description)
  {
    if (description != null)
    {
      for (QueryStatus st : QueryStatus.values())
      {
        if (description.equalsIgnoreCase(
            st.getDescription()))
        {
          return st;
        }
      }
      return QueryStatus.NO_DATA;
    }
    return null;
  }
}
