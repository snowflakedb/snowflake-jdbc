package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

/** Converter from Arrow VarBinaryVector to Binary. */
public class VarBinaryToBinaryConverter extends AbstractArrowVectorConverter {
  private VarBinaryVector varBinaryVector;

  /**
   * @param valueVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public VarBinaryToBinaryConverter(
      ValueVector valueVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.BINARY.name(), valueVector, columnIndex, context);
    this.varBinaryVector = (VarBinaryVector) valueVector;
  }

  @Override
  public String toString(int index) {
    byte[] bytes = toBytes(index);
    SFBinary binary = new SFBinary(bytes);
    return bytes == null ? null : context.getBinaryFormatter().format(binary);
  }

  @Override
  public byte[] toBytes(int index) {
    return varBinaryVector.getObject(index);
  }

  @Override
  public Object toObject(int index) {
    return toBytes(index);
  }

  @Override
  public boolean toBoolean(int index) throws SFException {
    String str = toString(index);
    if (str == null) {
      return false;
    } else {
      throw new SFException(ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, "Boolean", str);
    }
  }
}
