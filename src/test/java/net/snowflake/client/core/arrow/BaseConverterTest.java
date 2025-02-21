package net.snowflake.client.core.arrow;

import java.nio.ByteOrder;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;

public class BaseConverterTest implements DataConversionContext {
  private SnowflakeDateTimeFormat dateTimeFormat =
      SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD");
  private SnowflakeDateTimeFormat timeFormat = SnowflakeDateTimeFormat.fromSqlFormat("HH24:MI:SS");
  private SnowflakeDateTimeFormat timestampLTZFormat =
      SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS TZHTZM");
  private SnowflakeDateTimeFormat timestampNTZFormat =
      SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS TZHTZM");
  private SnowflakeDateTimeFormat timestampTZFormat =
      SnowflakeDateTimeFormat.fromSqlFormat("DY, DD MON YYYY HH24:MI:SS TZHTZM");

  private SFSession session = new SFSession();
  private int testScale = 9;
  private boolean honorClientTZForTimestampNTZ;
  protected final int invalidConversionErrorCode = ErrorCode.INVALID_VALUE_CONVERT.getMessageCode();

  @AfterEach
  public void clearTimeZone() {
    System.clearProperty("user.timezone");
  }

  @BeforeEach
  public void assumeLittleEndian() {
    Assumptions.assumeTrue(
        ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN),
        "Arrow doesn't support cross endianness");
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampLTZFormatter() {
    return timestampLTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampNTZFormatter() {
    return timestampNTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampTZFormatter() {
    return timestampTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getDateFormatter() {
    return dateTimeFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimeFormatter() {
    return timeFormat;
  }

  @Override
  public SFBinaryFormat getBinaryFormatter() {
    return SFBinaryFormat.BASE64;
  }

  public void setScale(int scale) {
    testScale = scale;
  }

  @Override
  public int getScale(int columnIndex) {
    return testScale;
  }

  @Override
  public SFSession getSession() {
    return session;
  }

  @Override
  public TimeZone getTimeZone() {
    return TimeZone.getDefault();
  }

  @Override
  public boolean getHonorClientTZForTimestampNTZ() {
    return honorClientTZForTimestampNTZ;
  }

  public void setHonorClientTZForTimestampNTZ(boolean val) {
    honorClientTZForTimestampNTZ = val;
  }

  @Override
  public long getResultVersion() {
    // Note: only cover current result version
    return 1;
  }
}
