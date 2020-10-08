/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
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
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TwoFieldStructToTimestampTZConverterTest extends BaseConverterTest {
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

  public TwoFieldStructToTimestampTZConverterTest(String tz) {
    System.setProperty("user.timezone", tz);
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int oldScale = 9;

  @Test
  public void testTimestampTZ() throws SFException {
    // test old and new dates
    long[] testEpochesInt64 = {1546391837, 1546391837, 0, 123, -12345, -12345678};

    int[] testScales = {0, 3, 0, 0, 0, 6};

    int[] testTimeZoneIndices = {960, 1440, 960, 960, 1440, 1440};

    String[] testTimesJson = {
      "1546391837.000000000 960",
      "1546391.8370000000 1440",
      "0.000000000 960",
      "123.000000000 960",
      "-12345.000000000 1440",
      "-12.345678000 1440"
    };

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIMESTAMP");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    FieldType fieldType2 =
        new FieldType(true, Types.MinorType.INT.getType(), null, customFieldMeta);

    StructVector structVector = StructVector.empty("testVector", allocator);
    List<Field> fieldList = new LinkedList<Field>();
    Field bigIntField =
        new Field(TwoFieldStructToTimestampTZConverter.FIELD_NAME_EPOCH, fieldType, null);

    Field smallIntField =
        new Field(
            TwoFieldStructToTimestampTZConverter.FIELD_NAME_TIME_ZONE_INDEX, fieldType2, null);

    fieldList.add(bigIntField);
    fieldList.add(smallIntField);

    structVector.initializeChildrenFromFields(fieldList);
    BigIntVector epoch =
        structVector.getChild(
            TwoFieldStructToTimestampTZConverter.FIELD_NAME_EPOCH, BigIntVector.class);
    IntVector timeZoneIdx =
        structVector.getChild(
            TwoFieldStructToTimestampTZConverter.FIELD_NAME_TIME_ZONE_INDEX, IntVector.class);

    int i = 0, j = 0;
    while (i < testEpochesInt64.length) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        epoch.setNull(j);
        timeZoneIdx.setNull(j);
        nullValIndex.add(j);
      } else {
        epoch.setSafe(j, testEpochesInt64[i]);
        timeZoneIdx.setSafe(j, testTimeZoneIndices[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter =
        new TwoFieldStructToTimestampTZConverter(structVector, 0, this);
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
                testTimesJson[i],
                oldScale,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ,
                getResultVersion(),
                getTimeZone(),
                getSession());
        Timestamp oldTs = sfTimestamp.getTimestamp();
        oldTs = ResultUtil.adjustTimestamp(oldTs);
        Date oldDate = new Date((oldTs).getTime());
        SFTimestamp sfTS =
            ResultUtil.getSFTimestamp(
                testTimesJson[i],
                oldScale,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ,
                getResultVersion(),
                getTimeZone(),
                getSession());
        String timestampStr =
            ResultUtil.getSFTimestampAsString(
                sfTS,
                SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ,
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
        i++;
        if (i < testScales.length) {
          this.setScale(testScales[i]);
        }
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBytes(x));
      }
      j++;
    }
    structVector.clear();
  }
}
