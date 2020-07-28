/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Timestamp with toString in UTC timezone. */
public class SnowflakeTimestampNTZAsUTC extends Timestamp {
  private static final long serialVersionUID = 1L;

  public SnowflakeTimestampNTZAsUTC(long seconds, int nanoseconds) {
    super(seconds);
    this.setNanos(nanoseconds);
  }

  public SnowflakeTimestampNTZAsUTC(Timestamp ts) {
    this(ts.getTime(), ts.getNanos());
  }

  /**
   * Returns a string representation in UTC
   *
   * @return a string representation of the object
   */
  public synchronized String toString() {
    int trailingZeros = 0;
    int tmpNanos = this.getNanos();
    if (tmpNanos == 0) {
      trailingZeros = 8;
    } else {
      while (tmpNanos % 10 == 0) {
        tmpNanos /= 10;
        trailingZeros++;
      }
    }
    final String baseFormat = "uuuu-MM-dd HH:mm:ss.";
    StringBuilder buf = new StringBuilder(baseFormat.length() + 9 - trailingZeros);
    buf.append(baseFormat);
    for (int i = 0; i < 9 - trailingZeros; ++i) {
      buf.append("S");
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(buf.toString());

    LocalDateTime ldt =
        LocalDateTime.ofEpochSecond(this.getTime() / 1000, this.getNanos(), ZoneOffset.UTC);
    return ldt.format(formatter);
  }
}
