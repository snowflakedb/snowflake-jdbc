package net.snowflake.client.jdbc;

import java.sql.Time;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class SnowflakeTimeAsWallclock extends Time {

    int nanos = 0;

    public SnowflakeTimeAsWallclock(long time, int nanos) {
        super(time);
        this.nanos = nanos;
    }

   /* public SnowflakeTimeAsWallclock(Time ts) {
        this(ts.getTime(), 0);
    }*/

    /**
     * Returns a string representation in UTC so as to display "wallclock time"
     *
     * @return a string representation of the object
     */
    public synchronized String toString() {
        int trailingZeros = 0;
        int tmpNanos = this.nanos;
        if (tmpNanos > 0) {
            while (tmpNanos % 10 == 0) {
                tmpNanos /= 10;
                trailingZeros++;
            }
        }
        String basicBaseFormat = "HH:mm:ss";
        String baseFormat = basicBaseFormat;
        if (tmpNanos > 0)
        {
            StringBuilder buf = new StringBuilder(basicBaseFormat.length() + 9 - trailingZeros + 1);
            buf.append(basicBaseFormat);
            buf.append(".");
            for (int i = 0; i < 9 - trailingZeros; ++i) {
                buf.append("S");
            }
            baseFormat = buf.toString();
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(baseFormat);
        LocalDateTime ldt =
                LocalDateTime.ofEpochSecond(this.getTime() / 1000, this.nanos, ZoneOffset.UTC);
        return ldt.format(formatter);
    }
}
