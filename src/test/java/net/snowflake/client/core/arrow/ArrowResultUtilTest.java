/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.util.Random;
import java.util.TimeZone;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class ArrowResultUtilTest {
  // test on multiple time zones
  @Parameterized.Parameters
  public static Object[][] data() {
    return new Object[][] {
      {"UTC"}, {"America/Los_Angeles"}, {"America/New_York"}, {"Asia/Singapore"}, {"MEZ"},
    };
  }

  @After
  public void clearTimeZone() {
    System.clearProperty("user.timezone");
  }

  public ArrowResultUtilTest(String tz) {
    System.setProperty("user.timezone", tz);
  }

  @Test
  @Ignore
  /** This is to show we can have 30X improvement using new API */
  public void testGetDatePerformance() throws SFException {
    Random random = new Random();
    int dateBound = 50000;
    int times = 100000;
    SFSession session = new SFSession();
    long start = System.currentTimeMillis();
    TimeZone tz = TimeZone.getDefault();
    int[] days = new int[times];
    for (int i = 0; i < times; i++) {
      days[i] = random.nextInt(dateBound) - dateBound / 2;
    }

    for (int i = 0; i < times; i++) {
      ResultUtil.getDate(Integer.toString(days[i]), tz, session);
    }
    long duration1 = System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      ArrowResultUtil.getDate(days[i], tz, tz);
    }
    long duration2 = System.currentTimeMillis() - start;

    start = System.currentTimeMillis();
    for (int i = 0; i < times; i++) {
      ArrowResultUtil.getDate(days[i]);
    }
    long duration3 = System.currentTimeMillis() - start;
    System.out.println(duration1 + " " + duration2 + " " + duration3);
  }

  @Test
  public void testToJavaTimestamp() {
    // ex: -1.123456789, -0.123456789, 0.123456789, 123.123456789, -123.123456789
    long[] cases = {-1123456789, -123456789, 123456789, 123123456789l, -123123456789l};
    long[] millsecs = {-1124, -124, 123, 123123, -123124};
    int[] nanos = {876543211, 876543211, 123456789, 123456789, 876543211};
    int scale = 9;
    for (int i = 0; i < cases.length; i++) {
      Timestamp ts = ArrowResultUtil.toJavaTimestamp(cases[i], scale);
      assertEquals(millsecs[i], ts.getTime());
      assertEquals(nanos[i], ts.getNanos());
    }
  }
}
