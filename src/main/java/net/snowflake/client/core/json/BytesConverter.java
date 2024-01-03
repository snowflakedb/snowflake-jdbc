package net.snowflake.client.core.json;

import java.nio.ByteBuffer;
import java.sql.Types;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFBinary;
import org.apache.arrow.vector.Float8Vector;

public class BytesConverter {
  private final Converters converters;

  BytesConverter(Converters converters) {
    this.converters = converters;
  }

  public byte[] getBytes(Object obj, int columnType, int columnSubType, Integer scale)
      throws SFException {
    if (obj == null) {
      return null;
    }

    try {
      // For all types except time/date/timestamp data, convert data into byte array. Different
      // methods are needed
      // for different types.
      switch (columnType) {
        case Types.FLOAT:
        case Types.DOUBLE:
          return ByteBuffer.allocate(Float8Vector.TYPE_WIDTH)
              .putDouble(0, converters.getNumberConverter().getDouble(obj, columnType))
              .array();
        case Types.NUMERIC:
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.BIGINT:
          return converters
              .getNumberConverter()
              .getBigDecimal(obj, columnType, scale)
              .toBigInteger()
              .toByteArray();
        case Types.VARCHAR:
        case Types.CHAR:
          return converters
              .getStringConverter()
              .getString(obj, columnType, columnSubType, scale)
              .getBytes();
        case Types.BOOLEAN:
          return converters.getBooleanConverter().getBoolean(obj, columnType)
              ? new byte[] {1}
              : new byte[] {0};
        case Types.TIMESTAMP:
        case Types.TIME:
        case Types.DATE:
        case Types.DECIMAL:
          throw new SFException(
              ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BYTES_STR, obj);
        default:
          return SFBinary.fromHex(obj.toString()).getBytes();
      }
    } catch (IllegalArgumentException ex) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, columnType, SnowflakeUtil.BYTES_STR, obj);
    }
  }
}
