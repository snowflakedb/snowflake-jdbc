package net.snowflake.client.core;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/** Timestamp with toString in UTC timezone. */
public class TimestampNTZ extends Timestamp {
  public TimestampNTZ(Timestamp ts) {
    super(ts.getTime());
    this.setNanos(ts.getNanos());
  }

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
    StringBuffer buf = new StringBuffer(baseFormat.length() + 9 - trailingZeros);
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
