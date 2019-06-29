/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Date;
import java.util.Random;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ArrowResultUtilTest
{
  // test on multiple time zones
  @Parameterized.Parameters
  public static Object[][] data()
  {
    return new Object[][]{
        {"UTC"},
        {"America/Los_Angeles"},
        {"America/New_York"},
        {"Pacific/Honolulu"},
        {"Asia/Singapore"},
        {"MEZ"},
        {"MESZ"}
    };
  }

  public ArrowResultUtilTest(String tz)
  {
    System.setProperty("user.timezone", tz);
  }

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
      int day = random.nextInt(dateBound) - dateBound / 2;
      ResultUtil.getDate(Integer.toString(day), tz, session);
    }
    long duration1 = System.currentTimeMillis() - start;


    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++)
    {
      int day = random.nextInt(dateBound) - dateBound / 2;
      ArrowResultUtil.getDate(day, tz, session);
    }
    long duration2 = System.currentTimeMillis() - start;
    System.out.println(duration1 + " " + duration2);
  }

  /**
   * Note: better to test it in different local time zone
   *
   * @throws SFException
   */
  @Test
  public void testGetDate() throws SFException
  {
    int day = -8865;
    Date newDate = ArrowResultUtil.getDate(day, TimeZone.getDefault(), new SFSession());
    Date oldDate = ResultUtil.getDate(Integer.toString(day), TimeZone.getDefault(), new SFSession());
    assertEquals(newDate, oldDate);
  }
}
