package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;

/** Convert from Arrow Float8Vector to Real. */
public class DoubleToRealConverter extends AbstractArrowVectorConverter {
  private Float8Vector float8Vector;
  private ByteBuffer byteBuf = ByteBuffer.allocate(Float8Vector.TYPE_WIDTH);

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public DoubleToRealConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.REAL.name(), fieldVector, columnIndex, context);
    this.float8Vector = (Float8Vector) fieldVector;
  }

  @Override
  public double toDouble(int index) {
    if (float8Vector.isNull(index)) {
      return 0;
    } else {
      return float8Vector.getDataBuffer().getDouble(index * Float8Vector.TYPE_WIDTH);
    }
  }

  @Override
  public byte[] toBytes(int index) {
    if (isNull(index)) {
      return null;
    } else {
      byteBuf.putDouble(0, toDouble(index));
      return byteBuf.array();
    }
  }

  @Override
  public float toFloat(int index) {
    return (float) toDouble(index);
  }

  @Override
  public Object toObject(int index) {
    return isNull(index) ? null : toDouble(index);
  }

  @Override
  public String toString(int index) {
    return isNull(index) ? null : String.valueOf(toDouble(index));
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    if (isNull(index)) {
      return false;
    }
    double val = toDouble(index);
    throw new SFException(
        ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
        SnowflakeUtil.BOOLEAN_STR, val);
  }

  @Override
  public short toShort(int rowIndex) throws SFException {
    try {
      if (isNull(rowIndex)) {
        return 0;
      } else {
        return (short) toDouble(rowIndex);
      }
    } catch (ClassCastException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          logicalTypeStr,
          SnowflakeUtil.SHORT_STR,
          toObject(rowIndex));
    }
  }

  @Override
  public int toInt(int rowIndex) throws SFException {
    try {
      if (isNull(rowIndex)) {
        return 0;
      } else {
        return (int) toDouble(rowIndex);
      }
    } catch (ClassCastException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          logicalTypeStr,
          SnowflakeUtil.INT_STR,
          toObject(rowIndex));
    }
  }

  @Override
  public long toLong(int rowIndex) throws SFException {
    try {
      if (isNull(rowIndex)) {
        return 0;
      } else {
        return (long) toDouble(rowIndex);
      }
    } catch (ClassCastException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT,
          logicalTypeStr,
          SnowflakeUtil.LONG_STR,
          toObject(rowIndex));
    }
  }

  @Override
  public BigDecimal toBigDecimal(int rowIndex) throws SFException {
    if (isNull(rowIndex)) {
      return null;
    }
    return BigDecimal.valueOf(toDouble(rowIndex));
  }
}
