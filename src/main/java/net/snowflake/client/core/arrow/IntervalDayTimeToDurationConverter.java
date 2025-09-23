package net.snowflake.client.core.arrow;

import java.time.Duration;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

class IntervalDayTimeToDurationConverter extends AbstractArrowVectorConverter {

  private BigIntVector vector;
  private static final long nanoInSecond = 1_000_000_000;
  private static final long nanoInMinute = nanoInSecond * 60;
  private static final long nanoInHour = nanoInMinute * 60;
  private static final long nanoInDay = nanoInHour * 24;
  protected int sfScale;

  public IntervalDayTimeToDurationConverter(
      ValueVector vector, int idx, DataConversionContext context) {
    super(SnowflakeType.INTERVAL_DAY_TIME.name(), vector, idx, context);
    this.sfScale = context.getScale(idx + 1);
    this.vector = (BigIntVector) vector;
  }

  @Override
  public Duration toDuration(int index, int scale) throws SFException {
    if (isNull(index)) {
      return null;
    }
    long numNanos = vector.getObject(index);
    try {
      int sign = Long.signum(numNanos);
      if (sign < 0) {
        numNanos = Math.abs(numNanos);
      }
      long numDay = 0;
      long numHour = 0;
      long numMinute = 0;
      long numSecond = 0;
      long numNanoSecond = 0;
      if (scale == 3 || scale == 4 || scale == 5 || scale == 6) {
        // INTERVAL DAY TO {SECOND|MINUTE|HOUR|DAY}
        numDay = numNanos / nanoInDay;
        numHour = (numNanos / nanoInHour) % 24;
        numMinute = (numNanos / nanoInMinute) % 60;
        numSecond = (numNanos / nanoInSecond) % 60;
        numNanoSecond = numNanos % nanoInSecond;
      } else if (scale == 7 || scale == 8 || scale == 9) {
        // INTERVAL HOUR TO {SECOND|MINUTE|HOUR}
        numHour = numNanos / nanoInHour;
        numMinute = (numNanos / nanoInMinute) % 60;
        numSecond = (numNanos / nanoInSecond) % 60;
        numNanoSecond = numNanos % nanoInSecond;
      } else if (scale == 10 || scale == 11) {
        // INTERVAL MINUTE TO {SECOND|MINUTE}
        numMinute = numNanos / nanoInMinute;
        numSecond = (numNanos / nanoInSecond) % 60;
        numNanoSecond = numNanos % nanoInSecond;
      } else if (scale == 12) {
        numSecond = numNanos / nanoInSecond;
        numNanoSecond = numNanos % nanoInSecond;
      }
      String ISODuration = (sign < 0) ? "-P" : "P";
      ISODuration =
          ISODuration
              + numDay
              + "DT"
              + numHour
              + "H"
              + numMinute
              + "M"
              + numSecond
              + "."
              + numNanoSecond
              + "S";
      return Duration.parse(ISODuration);
    } catch (ArithmeticException e) {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Duration", numNanos);
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    return toDuration(index, sfScale).toString();
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toDuration(index, sfScale);
  }
}
