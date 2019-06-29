package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SFBinaryFormat;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

import java.util.TimeZone;

public class BaseConverterTest implements DataConversionContext
{
  private SnowflakeDateTimeFormat dateTimeFormat = new SnowflakeDateTimeFormat("YYYY-MM-DD");
  private SnowflakeDateTimeFormat timeFormat = new SnowflakeDateTimeFormat("HH24:MI:SS");
  private SFSession session = new SFSession();
  private int testScale = 9;

  @Override
  public SnowflakeDateTimeFormat getTimestampLTZFormatter()
  {
    // TODO
    return null;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampNTZFormatter()
  {
    // TODO
    return null;
  }

  @Override
  public SnowflakeDateTimeFormat getTimestampTZFormatter()
  {
    // TODO
    return null;
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
    // TODO
    return false;
  }

  @Override
  public long getResultVersion()
  {
    // TODO
    return 0;
  }
}
