package net.snowflake.client.core.arrow;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

class DecfloatToDecimalConverter extends AbstractArrowVectorConverter {

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
    return toBigDecimal(index).toEngineeringString();
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
