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

  /**
   * Max unsigned plain-decimal <em>characters</em> (including {@code '.'} and a leading {@code
   * "0."}), matching ODBC {@code format_decfloat}. This is not "38 significant digits": a 38-digit
   * integer stays plain, but the same digits with a fraction (the {@code '.'} makes 39 chars) go
   * scientific.
   */
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
    return formatDecfloat(toBigDecimal(index));
  }

  /**
   * Formats a DECFLOAT string per SNOW-3229469.
   *
   * <ul>
   *   <li>{@code 0} for zero
   *   <li>plain decimal when the unsigned form fits in {@link #MAX_PLAIN_DIGITS} characters ({@code
   *       "123.456"}, {@code "100"})
   *   <li>otherwise normalized scientific notation: one non-zero digit before the decimal point,
   *       lowercase {@code e}, no {@code +} on the exponent ({@code "1.2e200"} not {@code "12e199"}
   *       / {@code "1.2E+200"})
   * </ul>
   */
  static String formatDecfloat(BigDecimal value) {
    if (value.signum() == 0) {
      return "0";
    }
    BigDecimal stripped = value.stripTrailingZeros();
    if (fitsInPlainDecimal(stripped)) {
      return stripped.toPlainString();
    }
    return toNormalizedScientific(stripped);
  }

  /**
   * True when the unsigned plain-decimal form is at most {@link #MAX_PLAIN_DIGITS} characters.
   * Length is computed from {@code precision}/{@code scale} so large-exponent values (e.g. {@code
   * 1e16384}) do not allocate a multi-kilobyte {@code toPlainString()} just to measure it.
   */
  private static boolean fitsInPlainDecimal(BigDecimal value) {
    int precision = value.precision();
    int scale = value.scale();
    int unsignedLength;
    if (scale <= 0) {
      unsignedLength = precision - scale;
    } else if (precision > scale) {
      unsignedLength = precision + 1;
    } else {
      unsignedLength = scale + 2;
    }
    return unsignedLength <= MAX_PLAIN_DIGITS;
  }

  /**
   * {@code d.ddddeN}: coefficient has exactly one non-zero digit before the decimal point; the
   * exponent is adjusted to match.
   */
  private static String toNormalizedScientific(BigDecimal value) {
    String digits = value.unscaledValue().abs().toString();
    int exponent = digits.length() - 1 - value.scale();

    StringBuilder result = new StringBuilder();
    if (value.signum() < 0) {
      result.append('-');
    }
    result.append(digits.charAt(0));
    if (digits.length() > 1) {
      result.append('.').append(digits, 1, digits.length());
    }
    result.append('e').append(exponent);
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
