/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.junit.After;

import java.util.TimeZone;

public class BaseConverterTest implements DataConversionContext
{
  private SnowflakeDateTimeFormat dateTimeFormat = SnowflakeDateTimeFormat.fromSqlFormat("YYYY-MM-DD");
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
  protected final int invalidConversionErrorCode =
      ErrorCode.INVALID_VALUE_CONVERT.getMessageCode();

  @After
  public void clearTimeZone()
  {
    System.clearProperty("user.timezone");
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampLTZFormatter()
  {
    return timestampLTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampNTZFormatter()
  {
    return timestampNTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampTZFormatter()
  {
    return timestampTZFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getDateFormatter()
  {
    return dateTimeFormat;
  }

  @Override
  public SnowflakeDateTimeFormat getTimeFormatter()
  {
    return timeFormat;
  }

  @Override
  public SFBinaryFormat getBinaryFormatter()
  {
    return SFBinaryFormat.BASE64;
  }

  public void setScale(int scale)
  {
    testScale = scale;
  }

  @Override
  public int getScale(int columnIndex)
  {
    return testScale;
  }

  @Override
  public SFSession getSession()
  {
    return session;
  }

  @Override
  public TimeZone getTimeZone()
  {
    return TimeZone.getDefault();
  }

  @Override
  public boolean getHonorClientTZForTimestampNTZ()
  {
    return honorClientTZForTimestampNTZ;
  }

  public void setHonorClientTZForTimestampNTZ(boolean val)
  {
    honorClientTZForTimestampNTZ = val;
  }

  @Override
  public long getResultVersion()
  {
    // Note: only cover current result version
    return 1;
  }
}
