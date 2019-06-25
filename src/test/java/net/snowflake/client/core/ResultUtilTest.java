package net.snowflake.client.core;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.TimeZone;

public class ResultUtilTest
{
  @Test
  @Ignore
  /**
   * This is to show we can have 30X improvement using new API
   */
  public void testGetDatePerformance() throws SFException
  {
    Random random = new Random();
    int dateBound = 50000;
    int times = 10000;
    SFSession session = new SFSession();
    long start = System.currentTimeMillis();
    TimeZone tz = TimeZone.getDefault();
    for (int i = 0; i < times; i++)
    {
      int day = random.nextInt(dateBound) - dateBound/2;
      ResultUtil.getDate(Integer.toString(day), tz, session);
    }
    long duration1 = System.currentTimeMillis() - start;


    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++)
    {
      int day = random.nextInt(dateBound) - dateBound/2;
      ResultUtil.getDate(day, tz);
    }
    long duration2 = System.currentTimeMillis() - start;
    System.out.println(duration1 + " " + duration2);
  }
}
