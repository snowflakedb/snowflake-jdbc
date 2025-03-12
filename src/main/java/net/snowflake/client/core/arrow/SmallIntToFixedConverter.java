package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;

/** Data vector whose snowflake logical type is fixed while represented as a short value vector */
public class SmallIntToFixedConverter extends AbstractArrowVectorConverter {
  protected int sfScale;
  protected SmallIntVector smallIntVector;
  ByteBuffer byteBuf = ByteBuffer.allocate(SmallIntVector.TYPE_WIDTH);

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public SmallIntToFixedConverter(
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
    this.smallIntVector = (SmallIntVector) fieldVector;
  }

  protected short getShort(int index) throws SFException {
    return smallIntVector.getDataBuffer().getShort(index * SmallIntVector.TYPE_WIDTH);
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else {
      byteBuf.putShort(0, getShort(index));
      return byteBuf.array();
    }
  }

  @Override
  public byte toByte(int index) throws SFException {
    short shortVal = toShort(index);
    byte byteVal = (byte) shortVal;

    if (byteVal == shortVal) {
      return byteVal;
    }
    throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "byte", shortVal);
  }

  @Override
  public short toShort(int index) throws SFException {
    if (smallIntVector.isNull(index)) {
      return 0;
    } else {
      return getShort(index);
    }
  }

  @Override
  public int toInt(int index) throws SFException {
    return (int) toShort(index);
  }

  @Override
  public long toLong(int index) throws SFException {
    return (long) toShort(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index) throws SFException {
    if (smallIntVector.isNull(index)) {
      return null;
    } else {
      return BigDecimal.valueOf((long) getShort(index), sfScale);
    }
  }

  @Override
  public float toFloat(int index) throws SFException {
    return toShort(index);
  }

  @Override
  public double toDouble(int index) throws SFException {
    return toFloat(index);
  }

  @Override
  public Object toObject(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else if (!shouldTreatDecimalAsInt()) {
      return BigDecimal.valueOf((long) getShort(index), sfScale);
    }
    return (long) getShort(index);
  }

  @Override
  public String toString(int index) throws SFException {
    return isNull(index) ? null : Short.toString(getShort(index));
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    short val = toShort(index);
    if (val == 0) {
      return false;
    } else if (val == 1) {
      return true;
    } else {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Boolean", val);
    }
  }
}
