package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.ValueVector;

/**
 * Data vector whose snowflake logical type is fixed while represented as a scaled short value
 * vector
 */
public class SmallIntToScaledFixedConverter extends SmallIntToFixedConverter {
  private String format;

  public SmallIntToScaledFixedConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context, int sfScale) {
    super(fieldVector, columnIndex, context);
    logicalTypeStr =
        String.format(
            "%s(%s,%s)",
            SnowflakeType.FIXED,
            fieldVector.getField().getMetadata().get("precision"),
            fieldVector.getField().getMetadata().get("scale"));
    format = ArrowResultUtil.getStringFormat(sfScale);
    this.sfScale = sfScale;
  }

  @Override
  public float toFloat(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    return ((float) getShort(index)) / ArrowResultUtil.powerOfTen(sfScale);
  }

  @Override
  public short toShort(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    float val = toFloat(index);
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Short", val);
  }

  @Override
  public int toInt(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    float val = toFloat(index);
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Int", val);
  }

  @Override
  public long toLong(int index) throws SFException {
    if (isNull(index)) {
      return 0;
    }
    float val = toFloat(index);
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Long", val);
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toBigDecimal(index);
  }

  @Override
  public String toString(int index) throws SFException {
    return isNull(index)
        ? null
        : String.format(format, (float) getShort(index) / ArrowResultUtil.powerOfTen(sfScale));
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
}
