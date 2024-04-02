package net.snowflake.client.core.json;

import static org.junit.Assert.assertArrayEquals;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Types;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import org.apache.arrow.vector.Float8Vector;
import org.junit.Test;

public class BytesConverterTest {
  private final Converters converters =
      new Converters(
          null, new SFSession(), 0, false, false, false, false, null, null, null, null, null, null);
  private final BytesConverter bytesConverter = new BytesConverter(converters);

  @Test
  public void testConvertFloatingPointToBytes() throws SFException {
    byte[] expected = ByteBuffer.allocate(Float8Vector.TYPE_WIDTH).putDouble(0, 12.5).array();
    assertArrayEquals(expected, bytesConverter.getBytes(12.5f, Types.FLOAT, Types.FLOAT, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12.5f, Types.DOUBLE, Types.DOUBLE, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12.5, Types.FLOAT, Types.DOUBLE, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12.5, Types.DOUBLE, Types.DOUBLE, 0));
  }

  @Test
  public void testConvertIntegerNumberToBytes() throws SFException {
    byte[] expected = new BigInteger("12").toByteArray();
    assertArrayEquals(expected, bytesConverter.getBytes(12, Types.NUMERIC, Types.NUMERIC, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12, Types.INTEGER, Types.INTEGER, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12, Types.SMALLINT, Types.SMALLINT, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12, Types.TINYINT, Types.TINYINT, 0));
    assertArrayEquals(expected, bytesConverter.getBytes(12, Types.BIGINT, Types.BIGINT, 0));
  }

  @Test
  public void testString() throws SFException {
    assertArrayEquals(
        "abc".getBytes(), bytesConverter.getBytes("abc", Types.VARCHAR, Types.VARCHAR, 0));
  }

  @Test
  public void testConvertBooleanToBytes() throws SFException {
    assertArrayEquals(
        new byte[] {1}, bytesConverter.getBytes(true, Types.BOOLEAN, Types.BOOLEAN, 0));
    assertArrayEquals(
        new byte[] {0}, bytesConverter.getBytes(false, Types.BOOLEAN, Types.BOOLEAN, 0));
  }
}
