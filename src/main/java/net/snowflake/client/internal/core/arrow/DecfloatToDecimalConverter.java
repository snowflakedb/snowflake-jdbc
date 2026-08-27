package net.snowflake.client.internal.core.arrow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.core.DataConversionContext;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

class DecfloatToDecimalConverter extends AbstractArrowVectorConverter {

  /** DECFLOAT precision; unsigned plain form is used when it fits in this many characters. */
  static final int MAX_PLAIN_DIGITS = 38;

  private StructVector vector;

  public DecfloatToDecimalConverter(ValueVector vector, int idx, DataConversionContext context) {
    super(SnowflakeType.DECFLOAT.name(), vector, idx, context);
    this.vector = (StructVector) vector;
  }

  @Override
  public BigDecimal toBigDecimal(int index) {
    if (isNull(index)) {
      return null;
    }
    Map<String, Object> value = (Map<String, Object>) vector.getObject(index);
    byte[] significandBytes = (byte[]) value.get("significand");
    short exponent = (short) value.get("exponent");
    BigInteger significand = new BigInteger(significandBytes);
    return new BigDecimal(significand, -exponent);
  }

  @Override
  public double toDouble(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    return toBigDecimal(rowIndex).doubleValue();
  }

  @Override
  public float toFloat(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    return toBigDecimal(rowIndex).floatValue();
  }

  @Override
  public short toShort(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(rowIndex);
    if (bigDecimal.scale() == 0) {
      short shortVal = bigDecimal.shortValue();
      if (shortVal == bigDecimal.longValue()) {
        return shortVal;
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
  public int toInt(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(rowIndex);
    if (bigDecimal.scale() == 0) {
      int intVal = bigDecimal.intValue();
      if (intVal == bigDecimal.longValue()) {
        return intVal;
      } else {
        throw new SFException(
            ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Integer", bigDecimal.toPlainString());
      }
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Integer", bigDecimal.toPlainString());
    }
  }

  @Override
  public long toLong(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return 0;
    }
    BigDecimal bigDecimal = toBigDecimal(rowIndex);
    if (bigDecimal.scale() == 0) {
      BigInteger intVal = bigDecimal.toBigIntegerExact();
      if (intVal.bitLength() <= 63) {
        return intVal.longValue();
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
  public Object toObject(int index) throws SFException {
    return toBigDecimal(index);
  }

  @Override
  public String toString(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    BigDecimal value = toBigDecimal(index);
    return formatDecfloat(value.unscaledValue(), -value.scale());
  }

  /**
   * Formats a DECFLOAT as a string, matching ODBC {@code format_decfloat}.
   *
   * <p>Uses plain decimal when the unsigned form fits in {@link #MAX_PLAIN_DIGITS} characters, and
   * normalized scientific notation otherwise. Scientific notation uses a single non-zero digit
   * before the decimal point, lowercase {@code e}, and no {@code +} on positive exponents.
   *
   * @param significand unscaled integer significand
   * @param exponent power of ten such that the value is {@code significand * 10^exponent}
   * @return formatted DECFLOAT string
   */
  static String formatDecfloat(BigInteger significand, int exponent) {
    if (significand.signum() == 0) {
      return "0";
    }

    boolean negative = significand.signum() < 0;
    BigInteger absSig = significand.abs();
    int exp = exponent;
    while (absSig.mod(BigInteger.TEN).signum() == 0) {
      absSig = absSig.divide(BigInteger.TEN);
      exp++;
    }

    String digits = absSig.toString();
    int n = digits.length();
    int plainLen;
    if (exp >= 0) {
      plainLen = n + exp;
    } else {
      int absExp = -exp;
      if (absExp < n) {
        plainLen = n + 1;
      } else {
        plainLen = 2 + absExp;
      }
    }

    StringBuilder result = new StringBuilder();
    if (plainLen <= MAX_PLAIN_DIGITS) {
      if (exp >= 0) {
        result.append(digits);
        for (int i = 0; i < exp; i++) {
          result.append('0');
        }
      } else {
        int absExp = -exp;
        if (absExp < n) {
          int decimalPos = n - absExp;
          result.append(digits, 0, decimalPos);
          result.append('.');
          result.append(digits, decimalPos, n);
        } else {
          int leadingZeros = absExp - n;
          result.append("0.");
          for (int i = 0; i < leadingZeros; i++) {
            result.append('0');
          }
          result.append(digits);
        }
      }
    } else {
      long adjustedExp = (long) exp + n - 1;
      result.append(digits.charAt(0));
      if (n > 1) {
        result.append('.');
        result.append(digits, 1, n);
      }
      result.append('e');
      result.append(adjustedExp);
    }

    if (negative) {
      result.insert(0, '-');
    }
    return result.toString();
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.BYTES_STR, null);
  }

  @Override
  public boolean toBoolean(int rowIndex) throws SFException {
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.BOOLEAN_STR, null);
  }
}
