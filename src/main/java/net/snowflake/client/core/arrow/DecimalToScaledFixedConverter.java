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

/**
 * Data vector whose snowflake logical type is fixed while represented as a BigDecimal value vector
 */
public class DecimalToScaledFixedConverter extends AbstractArrowVectorConverter {
  protected DecimalVector decimalVector;
  private static final BigDecimal nanoInSecond = BigDecimal.valueOf(1_000_000_000);
  private static final BigDecimal nanoInMinute = nanoInSecond.multiply(BigDecimal.valueOf(60));
  private static final BigDecimal nanoInHour = nanoInMinute.multiply(BigDecimal.valueOf(60));
  private static final BigDecimal nanoInDay = nanoInHour.multiply(BigDecimal.valueOf(24));

  /**
   * @param fieldVector ValueVector
   * @param vectorIndex vector index
   * @param context DataConversionContext
   */
  public DecimalToScaledFixedConverter(
      ValueVector fieldVector, int vectorIndex, DataConversionContext context) {
    super(
        String.format(
            "%s(%s,%s)",
            SnowflakeType.FIXED,
            fieldVector.getField().getMetadata().get("precision"),
            fieldVector.getField().getMetadata().get("scale")),
        fieldVector,
        vectorIndex,
        context);
    decimalVector = (DecimalVector) fieldVector;
  }

  @Override
  public byte[] toBytes(int index) {
    if (isNull(index)) {
      return null;
    } else {
      return toBigDecimal(index).toBigInteger().toByteArray();
    }
  }

  @Override
  public byte toByte(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }

    BigDecimal bigDecimal = toBigDecimal(index);
    if (bigDecimal.scale() == 0) {
      byte byteVal = bigDecimal.byteValue();

      if (byteVal == bigDecimal.longValue()) {
        return byteVal;
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Byte", bigDecimal.toPlainString());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Byte", bigDecimal.toPlainString());
    }
  }

  @Override
  public short toShort(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(index);
    if (bigDecimal.scale() == 0) {
      short shortValue = bigDecimal.shortValue();

      if (bigDecimal.compareTo(BigDecimal.valueOf((long) shortValue)) == 0) {
        return shortValue;
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Short", bigDecimal.toPlainString());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Short", bigDecimal.toPlainString());
    }
  }

  @Override
  public int toInt(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(index);
    if (bigDecimal.scale() == 0) {
      int intValue = bigDecimal.intValue();

      if (bigDecimal.compareTo(BigDecimal.valueOf((long) intValue)) == 0) {
        return intValue;
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Int", bigDecimal.toPlainString());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Int", bigDecimal.toPlainString());
    }
  }

  @Override
  public long toLong(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(index);
    if (bigDecimal.scale() == 0) {
      long longValue = bigDecimal.longValue();

      if (bigDecimal.compareTo(BigDecimal.valueOf(longValue)) == 0) {
        return longValue;
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Long", bigDecimal.toPlainString());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Long", bigDecimal.toPlainString());
    }
  }

  @Override
  public float toFloat(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(index);
    return bigDecimal.floatValue();
  }

  @Override
  public double toDouble(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(index);
    return bigDecimal.doubleValue();
  }

  @Override
  public BigDecimal toBigDecimal(int index) {
    return decimalVector.getObject(index);
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toBigDecimal(index);
  }

  @Override
  public String toString(int index) {
    BigDecimal bigDecimal = toBigDecimal(index);
    return bigDecimal == null ? null : bigDecimal.toPlainString();
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    BigDecimal val = toBigDecimal(index);
    if (val.compareTo(BigDecimal.ZERO) == 0) {
      return false;
    } else if (val.compareTo(BigDecimal.ONE) == 0) {
      return true;
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Boolean", val.toPlainString());
    }
  }

  @Override
  public Duration toDuration(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    BigDecimal numNanos = toBigDecimal(index);
    try {
      int sign = numNanos.signum();
      if (sign < 0) {
        numNanos = numNanos.abs();
      }
      long numDay = numNanos.divide(nanoInDay, RoundingMode.FLOOR).longValueExact();
      long numHour = (numNanos.divide(nanoInHour, RoundingMode.FLOOR).longValueExact()) % 24;
      long numMinute = (numNanos.divide(nanoInMinute, RoundingMode.FLOOR).longValueExact()) % 60;
      long numSecond = (numNanos.divide(nanoInSecond, RoundingMode.FLOOR).longValueExact()) % 60;
      long numNanoSecond = numNanos.remainder(nanoInSecond).longValueExact();
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
}
