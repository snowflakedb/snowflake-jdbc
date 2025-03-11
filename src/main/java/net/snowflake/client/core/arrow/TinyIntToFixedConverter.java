package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;

/** A converter from arrow tinyint to Snowflake Fixed type converter */
public class TinyIntToFixedConverter extends AbstractArrowVectorConverter {
  protected TinyIntVector tinyIntVector;
  protected int sfScale = 0;

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public TinyIntToFixedConverter(
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
    this.tinyIntVector = (TinyIntVector) fieldVector;
  }

  @Override
  public byte toByte(int index) throws SFException {
    if (tinyIntVector.isNull(index)) {
      return 0;
    } else {
      return getByte(index);
    }
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    if (tinyIntVector.isNull(index)) {
      return null;
    }
    ByteBuffer bytes = ByteBuffer.allocate(TinyIntVector.TYPE_WIDTH);
    tinyIntVector.getDataBuffer().getBytes(index, bytes);
    return bytes.array();
  }

  protected byte getByte(int index) throws SFException {
    return tinyIntVector.getDataBuffer().getByte(index * TinyIntVector.TYPE_WIDTH);
  }

  @Override
  public short toShort(int index) throws SFException {
    return (short) toByte(index);
  }

  @Override
  public int toInt(int index) throws SFException {
    return (int) toByte(index);
  }

  @Override
  public float toFloat(int index) throws SFException {
    return toByte(index);
  }

  @Override
  public double toDouble(int index) throws SFException {
    return toFloat(index);
  }

  @Override
  public long toLong(int index) throws SFException {
    return (long) toByte(index);
  }

  @Override
  public BigDecimal toBigDecimal(int index) throws SFException {
    if (tinyIntVector.isNull(index)) {
      return null;
    } else {
      return BigDecimal.valueOf((long) getByte(index), sfScale);
    }
  }

  @Override
  public Object toObject(int index) throws SFException {
    if (isNull(index)) {
      return null;
    } else if (!shouldTreatDecimalAsInt()) {
      return BigDecimal.valueOf((long) getByte(index), sfScale);
    }
    return (long) toByte(index);
  }

  @Override
  public String toString(int index) throws SFException {
    return isNull(index) ? null : Short.toString(getByte(index));
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
