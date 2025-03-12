package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.ValueVector;

/** Data vector whose snowflake logical type is fixed while represented as a long value vector */
public class BigIntToFixedConverter extends AbstractArrowVectorConverter {
  /** Underlying vector that this converter will convert from */
  protected BigIntVector bigIntVector;

  /** scale of the fixed value */
  protected int sfScale;

  protected ByteBuffer byteBuf = ByteBuffer.allocate(BigIntVector.TYPE_WIDTH);

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public BigIntToFixedConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(
        String.format(
            "%s(%s,%s)",
            SnowflakeType.FIXED,
            fieldVector.getField().getMetadata().get("precision"),
            fieldVector.getField().getMetadata().get("scale")),
        fieldVector,
        columnIndex,
        context);
    this.bigIntVector = (BigIntVector) fieldVector;
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

  @Override
  public boolean toBoolean(int index) throws SFException {
    long longVal = toLong(index);
    if (longVal == 0) {
      return false;
    } else if (longVal == 1) {
      return true;
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
          SnowflakeUtil.BOOLEAN_STR, longVal);
    }
  }

  @Override
  public byte toByte(int index) throws SFException {
    long longVal = toLong(index);
    byte byteVal = (byte) longVal;

    if (byteVal == longVal) {
      return byteVal;
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr,
          SnowflakeUtil.BYTE_STR, longVal);
    }
  }

  @Override
  public short toShort(int index) throws SFException {
    long longVal = toLong(index);
    short shortVal = (short) longVal;

    if (shortVal == longVal) {
      return shortVal;
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.SHORT_STR, longVal);
    }
  }

  @Override
  public int toInt(int index) throws SFException {
    long longVal = toLong(index);
    int intVal = (int) longVal;

    if (intVal == longVal) {
      return intVal;
    } else {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, SnowflakeUtil.INT_STR, longVal);
    }
  }

  protected long getLong(int index) {
    return bigIntVector.getDataBuffer().getLong(index * BigIntVector.TYPE_WIDTH);
  }

  @Override
  public long toLong(int index) throws SFException {
    if (bigIntVector.isNull(index)) {
      return 0;
    } else {
      return getLong(index);
    }
  }

  @Override
  public float toFloat(int index) throws SFException {
    return toLong(index);
  }

  @Override
  public double toDouble(int index) throws SFException {
    return toLong(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index) {
    if (bigIntVector.isNull(index)) {
      return null;
    } else {
      return BigDecimal.valueOf(getLong(index), sfScale);
    }
  }

  @Override
  public Object toObject(int index) throws SFException {
    if (bigIntVector.isNull(index)) {
      return null;
    } else if (!shouldTreatDecimalAsInt()) {
      return BigDecimal.valueOf(getLong(index), sfScale);
    }
    return getLong(index);
  }

  @Override
  public String toString(int index) {
    return isNull(index) ? null : Long.toString(getLong(index));
  }
}
