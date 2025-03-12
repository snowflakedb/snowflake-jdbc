package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

/** Convert Arrow BitVector to Boolean */
public class BitToBooleanConverter extends AbstractArrowVectorConverter {
  private BitVector bitVector;

  /**
   * @param fieldVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public BitToBooleanConverter(
      ValueVector fieldVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.BOOLEAN.name(), fieldVector, columnIndex, context);
    this.bitVector = (BitVector) fieldVector;
  }

  private int getBit(int index) {
    // read a bit from the bitVector
    // first find the byte value
    final int byteIndex = index >> 3;
    final byte b = bitVector.getDataBuffer().getByte(byteIndex);
    // then get the bit value
    final int bitIndex = index & 7;
    return (b >> bitIndex) & 0x01;
  }

  @Override
  public boolean toBoolean(int index) {
    if (isNull(index)) {
      return false;
    } else {
      return getBit(index) != 0;
    }
  }

  @Override
  public byte[] toBytes(int index) {
    if (isNull(index)) {
      return null;
    } else if (toBoolean(index)) {

      return new byte[] {1};
    } else {
      return new byte[] {0};
    }
  }

  @Override
  public Object toObject(int index) {
    return isNull(index) ? null : toBoolean(index);
  }

  @Override
  public String toString(int index) {
    return isNull(index) ? null : toBoolean(index) ? "TRUE" : "FALSE";
  }

  @Override
  public short toShort(int rowIndex) throws SFException {
    return (short) (toBoolean(rowIndex) ? 1 : 0);
  }

  @Override
  public int toInt(int rowIndex) throws SFException {
    return toBoolean(rowIndex) ? 1 : 0;
  }

  @Override
  public long toLong(int rowIndex) throws SFException {
    return toBoolean(rowIndex) ? 1 : 0;
  }

  @Override
  public float toFloat(int rowIndex) throws SFException {
    return toBoolean(rowIndex) ? 1 : 0;
  }

  @Override
  public double toDouble(int rowIndex) throws SFException {
    return toBoolean(rowIndex) ? 1 : 0;
  }

  @Override
  public BigDecimal toBigDecimal(int rowIndex) throws SFException {
    return isNull(rowIndex) ? null : toBoolean(rowIndex) ? BigDecimal.ONE : BigDecimal.ZERO;
  }
}
