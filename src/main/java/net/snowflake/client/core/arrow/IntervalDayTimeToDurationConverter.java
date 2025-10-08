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

  public IntervalDayTimeToDurationConverter(
      ValueVector vector, int idx, DataConversionContext context) {
    super(SnowflakeType.INTERVAL_DAY_TIME.name(), vector, idx, context);
    this.vector = (BigIntVector) vector;
  }

  @Override
  public Duration toDuration(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    long numNanos = vector.getObject(index);
    try {
      int sign = Long.signum(numNanos);
      numNanos = Math.abs(numNanos);
      // Duration.ofSeconds() with passed in negative second value results in overflow
      // so instead we identify the sign of numNanos and use Duration.negated() accordingly
      Duration duration = Duration.ofSeconds(numNanos / nanoInSecond, numNanos % nanoInSecond);
      if (sign >= 0) {
        return duration;
      } else {
        return duration.negated();
      }
    } catch (ArithmeticException e) {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Duration", numNanos);
    }
  }

  @Override
  public String toString(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    return toDuration(index).toString();
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toDuration(index);
  }
}
