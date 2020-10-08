/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.common.core.SFTimestamp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BigIntToTimestampLTZConverterTest extends BaseConverterTest {
  @Parameterized.Parameters
  public static Object[][] data() {
    return new Object[][] {
      {"UTC"},
      {"America/Los_Angeles"},
      {"America/New_York"},
      {"Pacific/Honolulu"},
      {"Asia/Singapore"},
      {"MEZ"},
      {"MESZ"}
    };
  }

  public BigIntToTimestampLTZConverterTest(String tz) {
    System.setProperty("user.timezone", tz);
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int oldScale = 9;

  @Test
  public void testTimestampLTZ() throws SFException {
    // test old and new dates
    long[] testTimestampsInt64 = {
      1546391837,
      15463918370l,
      154639183700l,
      1546391837000l,
      15463918370000l,
      154639183700000l,
      1546391837000000l
    };

    // test scale from seconds to microseconds
    int[] testScales = {0, 1, 2, 3, 4, 5, 6};

    String[] testTimesJson = {"1546391837.000000000"};

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIMESTAMP");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("timestamp", fieldType, allocator);
    int i = 0, j = 0;
    while (i < testTimestampsInt64.length) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(j);
        nullValIndex.add(j);
      } else {
        vector.setSafe(j, testTimestampsInt64[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter = new BigIntToTimestampLTZConverter(vector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;
    this.setScale(testScales[i]);
    while (j < rowCount) {
      Timestamp ts = converter.toTimestamp(j, getTimeZone());
      Date date = converter.toDate(j, getTimeZone(), false);
      Time time = converter.toTime(j);
      String tsStr = converter.toString(j);

      if (nullValIndex.contains(j)) {
        assertThat(ts, is(nullValue()));
        assertThat(date, is(nullValue()));
        assertThat(false, is(converter.toBoolean(j)));
        assertThat(converter.toBytes(j), is(nullValue()));
      } else {
        SFTimestamp sfTimestamp =
            ResultUtil.getSFTimestamp(
                testTimesJson[0],
                oldScale,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ,
                getResultVersion(),
                getTimeZone(),
                getSession());
        Timestamp oldTs = sfTimestamp.getTimestamp();
        oldTs = ResultUtil.adjustTimestamp(oldTs);
        Date oldDate = new Date((oldTs).getTime());
        SFTimestamp sfTS =
            ResultUtil.getSFTimestamp(
                testTimesJson[0],
                oldScale,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ,
                getResultVersion(),
                getTimeZone(),
                getSession());
        String timestampStr =
            ResultUtil.getSFTimestampAsString(
                sfTS,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ,
                oldScale,
                getTimestampNTZFormatter(),
                getTimestampLTZFormatter(),
                getTimestampTZFormatter(),
                getSession());
        Time oldTime = new Time(oldTs.getTime());
        assertThat(oldDate, is(date));
        assertThat(oldTs, is(ts));
        assertThat(oldTime, is(time));
        assertThat(timestampStr, is(tsStr));
        assertThat(converter.toBytes(j), is(notNullValue()));
        i++;
        if (i < testScales.length) {
          this.setScale(testScales[i]);
        }
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
      }
      j++;
    }
    vector.clear();
  }
}
