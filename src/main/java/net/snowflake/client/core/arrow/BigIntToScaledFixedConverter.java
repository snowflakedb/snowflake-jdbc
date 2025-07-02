package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;

/**
 * Data vector whose snowflake logical type is fixed while represented as a long value vector with
 * scale
 */
public class BigIntToScaledFixedConverter extends BigIntToFixedConverter {
  public BigIntToScaledFixedConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context, int scale) {
    super(fieldVector, columnIndex, context);
    logicalTypeStr =
        String.format(
            "%s(%s,%s)",
            SnowflakeType.FIXED,
            fieldVector.getField().getMetadata().get("precision"),
            fieldVector.getField().getMetadata().get("scale"));
    sfScale = scale;
  }

  @Override
  public float toFloat(int index) throws SFException {
    return (float) toDouble(index);
  }

  @Override
  public double toDouble(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    if (sfScale > 9) {
      return toBigDecimal(index).doubleValue();
    }
    int scale = sfScale;
    double res = getLong(index);
    res = res / ArrowResultUtil.powerOfTen(scale);
    return res;
  }

  @Override
  public short toShort(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal val = toBigDecimal(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Short", val.toPlainString());
  }

  @Override
  public int toInt(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal val = toBigDecimal(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Int", val.toPlainString());
  }

  @Override
  public long toLong(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    BigDecimal val = toBigDecimal(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Long", val.toPlainString());
  }

  @Override
  public Object toObject(int index) {
    return toBigDecimal(index);
  }

  @Override
  public String toString(int index) {
    return isNull(index) ? null : BigDecimal.valueOf(getLong(index), sfScale).toPlainString();
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
  public byte[] toBytes(int index) {
    if (isNull(index)) {
      return null;
    } else {
      byteBuf.putLong(0, getLong(index));
      return byteBuf.array();
    }
  }
}
