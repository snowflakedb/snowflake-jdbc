package net.snowflake.client.internal.core.arrow;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.resultset.SnowflakeType;
import net.snowflake.client.internal.core.DataConversionContext;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.util.IpAddressParser;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;

/**
 * Converts IPADDRESS columns, whose values are canonical IPv4/IPv6 text. {@link #toString} returns
 * that text; {@link #toObject} returns an {@link java.net.Inet4Address} or {@link
 * java.net.Inet6Address}.
 */
public class IpAddressConverter extends AbstractArrowVectorConverter {
  private final VarCharVector varCharVector;

  /**
   * @param valueVector ValueVector
   * @param columnIndex column index
   * @param context DataConversionContext
   */
  public IpAddressConverter(
      ValueVector valueVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.IPADDRESS.name(), valueVector, columnIndex, context);
    this.varCharVector = (VarCharVector) valueVector;
  }

  @Override
  public String toString(int index) {
    byte[] bytes = toBytes(index);
    return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
  }

  @Override
  public byte[] toBytes(int index) {
    return isNull(index) ? null : varCharVector.get(index);
  }

  @Override
  public Object toObject(int index) throws SFException {
    String text = toString(index);
    if (text == null) {
      return null;
    }
    InetAddress address = IpAddressParser.parse(text);
    if (address == null) {
      throw new SFException(
          ErrorCode.INVALID_VALUE_CONVERT, logicalTypeStr, IpAddressParser.INET_ADDRESS_STR, text);
    }
    return address;
  }
}
