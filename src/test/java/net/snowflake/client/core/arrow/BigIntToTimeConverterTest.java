/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import net.snowflake.client.TestUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Time;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class BigIntToTimeConverterTest extends BaseConverterTest
{
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

  public BigIntToTimeConverterTest(String tz)
  {
    System.setProperty("user.timezone", tz);
  }

  /**
   * allocator for arrow
   */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int scale = 9;

  @Test
  public void testTime() throws SFException
  {
    // test old and new dates
    long[] testTimesInt64 = {
        12345678000000L
    };

    String[] testTimesJson = {
        "12345.678000000"
    };

    Time[] expectedTimes = {
        new Time(12345678)
    };

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIME");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.BIGINT.getType(),
                                        null, customFieldMeta);

    BigIntVector vector = new BigIntVector("date", fieldType, allocator);
    int i = 0, j = 0;
    while (i < testTimesInt64.length)
    {
      boolean isNull = random.nextBoolean();
      if (isNull)
      {
        vector.setNull(j);
        nullValIndex.add(j);
      }
      else
      {
        vector.setSafe(j, testTimesInt64[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter = new BigIntToTimeConverter(vector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;
    while (j < rowCount)
    {
      String strVal = converter.toString(j);
      Time time = converter.toTime(j);
      Object obj = converter.toObject(j);
      Time oldTime =
          new Time(ResultUtil.getSFTime(
              testTimesJson[i], scale, new SFSession()).getFractionalSeconds(
              ResultUtil.DEFAULT_SCALE_OF_SFTIME_FRACTION_SECONDS));
      if (nullValIndex.contains(j))
      {
        assertThat(obj, is(nullValue()));
        assertThat(strVal, is(nullValue()));
        assertThat(false, is(converter.toBoolean(j)));
        assertThat(converter.toBytes(j), is(nullValue()));
      }
      else
      {
        assertThat(expectedTimes[i], is(time));
        assertThat(expectedTimes[i], is((Time) obj));
        assertThat(oldTime, is(time));
        assertThat(oldTime, is((Time) obj));
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode,
                                   () -> converter.toBoolean(x));
      }
      j++;
    }
    vector.clear();
  }
}
