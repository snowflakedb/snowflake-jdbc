package net.snowflake.client.core.json;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.sql.Types;
import java.time.ZoneId;
import java.util.TimeZone;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.SnowflakeResultSetSerializableV1;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.junit.Before;
import org.junit.Test;

public class StringConverterTest {
  private final TimeZone honoluluTimeZone =
      TimeZone.getTimeZone(ZoneId.of("America/Nuuk")); // session time zone
  private final SnowflakeResultSetSerializableV1 resultSetSerializableV1 =
      mock(SnowflakeResultSetSerializableV1.class);
  private Converters converters;

  private StringConverter stringConverter;

  @Before
  public void init() {
    SnowflakeDateTimeFormat timestampNTZFormatter =
        SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD HH24:MI:SS.FF3");
    SnowflakeDateTimeFormat timestampLTZFormatter =
        SnowflakeDateTimeFormat.fromSqlFormat("YYYY/DD/MM HH24:MI:SS.FF3");
    SnowflakeDateTimeFormat timestampTZFormatter =
        SnowflakeDateTimeFormat.fromSqlFormat("YYYY MM DD HH24:MI:SS.FF3");
    SnowflakeDateTimeFormat dateFormatter = SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD");
    SnowflakeDateTimeFormat timeFormatter = SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI:SS.FF3");
    converters =
        new Converters(
            honoluluTimeZone,
            new SFSession(),
            1,
            false,
            false,
            false,
            false,
            SFBinaryFormat.BASE64,
            dateFormatter,
            timeFormatter,
            timestampNTZFormatter,
            timestampLTZFormatter,
            timestampTZFormatter);
    stringConverter = converters.getStringConverter();
  }

  @Test
  public void testConvertingString() throws SFException {
    assertEquals("test", stringConverter.getString("test", Types.VARCHAR, Types.VARCHAR, 0));
  }

  @Test
  public void testConvertingBoolean() throws SFException {
    assertEquals("TRUE", stringConverter.getString(true, Types.BOOLEAN, Types.BOOLEAN, 0));
    assertEquals("TRUE", stringConverter.getString("true", Types.BOOLEAN, Types.BOOLEAN, 0));
    assertEquals("FALSE", stringConverter.getString(false, Types.BOOLEAN, Types.BOOLEAN, 0));
    assertEquals("FALSE", stringConverter.getString("false", Types.BOOLEAN, Types.BOOLEAN, 0));
  }

  @Test
  public void testConvertingNumbers() throws SFException {
    assertEquals("12", stringConverter.getString(12, Types.INTEGER, Types.INTEGER, 0));
    assertEquals("12", stringConverter.getString(12, Types.TINYINT, Types.TINYINT, 0));
    assertEquals("12", stringConverter.getString(12, Types.SMALLINT, Types.SMALLINT, 0));
    assertEquals("12", stringConverter.getString(12L, Types.BIGINT, Types.BIGINT, 0));
    assertEquals("12.5", stringConverter.getString(12.5, Types.DOUBLE, Types.DOUBLE, 0));
    assertEquals("12.5", stringConverter.getString(12.5F, Types.FLOAT, Types.FLOAT, 0));
  }

  @Test
  public void testConvertingTimestamp() throws SFException {
    assertEquals(
        "1988-03-21 22:33:15.000",
        stringConverter.getString("574986795", Types.TIMESTAMP, Types.TIMESTAMP, 0));
    assertEquals(
        "1988-03-21 19:33:15.000",
        stringConverter.getString(
            "574986795", Types.TIMESTAMP, SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ, 0));
    assertEquals(
        "1988-03-21 14:33:15.000",
        stringConverter.getString(
            "574986795 960", Types.TIMESTAMP, SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ, 0));
  }

  @Test
  public void testConvertingDate() throws SFException {
    assertEquals("2023-12-18", stringConverter.getString("19709", Types.DATE, Types.DATE, 0));
  }

  @Test
  public void testConvertingTime() throws SFException {
    assertEquals(
        "00:13:18.000", stringConverter.getString("798.838000000", Types.TIME, Types.TIME, 0));
  }

  @Test
  public void testConvertingBinary() throws SFException {
    assertEquals("AQID", stringConverter.getString("010203", Types.BINARY, Types.BINARY, 0));
  }
}
