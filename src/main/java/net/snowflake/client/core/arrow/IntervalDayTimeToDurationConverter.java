package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;

class IntervalDayTimeToDurationConverter extends AbstractArrowVectorConverter {

  private DecimalVector vector;

  public IntervalDayTimeToDurationConverter(
      ValueVector vector, int idx, DataConversionContext context) {
    super(SnowflakeType.INTERVAL_DAY_TIME.name(), vector, idx, context);
    this.vector = (DecimalVector) vector;
  }

  @Override
  public Duration toDuration(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    BigDecimal numNanos = vector.getObject(index);
    long nanoInSecond = 1_000_000_000;
    long nanoInMinute = nanoInSecond * 60;
    long nanoInHour = nanoInMinute * 60;
    long nanoInDay = nanoInHour * 24;
    try {
      int sign = numNanos.signum();
      if (sign < 0) {
        numNanos = numNanos.abs();
      }
      long numDay =
          numNanos.divide(BigDecimal.valueOf(nanoInDay), RoundingMode.FLOOR).longValueExact();
      long numHour =
          (numNanos.divide(BigDecimal.valueOf(nanoInHour), RoundingMode.FLOOR).longValueExact())
              % 24;
      long numMinute =
          (numNanos.divide(BigDecimal.valueOf(nanoInMinute), RoundingMode.FLOOR).longValueExact())
              % 60;
      long numSecond =
          (numNanos.divide(BigDecimal.valueOf(nanoInSecond), RoundingMode.FLOOR).longValueExact())
              % 60;
      long numNanoSecond = numNanos.remainder(BigDecimal.valueOf(nanoInSecond)).longValueExact();
      String ISODuration = (sign < 0) ? "-P" : "P";
      ISODuration =
          ISODuration
              + Long.toString(numDay)
              + "DT"
              + Long.toString(numHour)
              + "H"
              + Long.toString(numMinute)
              + "M"
              + Long.toString(numSecond)
              + "."
              + Long.toString(numNanoSecond)
              + "S";
      return Duration.parse(ISODuration);
    } catch (ArithmeticException e) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Duration", numNanos.toPlainString());
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
