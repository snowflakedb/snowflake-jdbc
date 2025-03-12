package net.snowflake.client.util;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeUtil;

/** Class keeping the start and stop time in epoch microseconds. */
@SnowflakeJdbcInternalApi
public class TimeMeasurement {
  private long start;
  private long end;

  /**
   * Get the start time as epoch time in microseconds.
   *
   * @return the start time as epoch time in microseconds.
   */
  public long getStart() {
    return start;
  }

  /** Set the start time as current epoch time in microseconds. */
  public void setStart() {
    this.start = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  /**
   * Get the stop time as epoch time in microseconds.
   *
   * @return the stop time as epoch time in microseconds.
   */
  public long getEnd() {
    return end;
  }

  /** Set the stop time as current epoch time in microseconds. */
  public void setEnd() {
    this.end = SnowflakeUtil.getEpochTimeInMicroSeconds();
  }

  /**
   * Get the microseconds between the stop and start time.
   *
   * @return difference between stop and start in microseconds. If one of the variables is not
   *     initialized, it returns -1
   */
  public long getTime() {
    if (start == 0 || end == 0) {
      return -1;
    }

    return end - start;
  }
}
