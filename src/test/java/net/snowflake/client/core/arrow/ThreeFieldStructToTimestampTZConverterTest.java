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
public class ThreeFieldStructToTimestampTZConverterTest extends BaseConverterTest {
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

  public ThreeFieldStructToTimestampTZConverterTest(String tz) {
    System.setProperty("user.timezone", tz);
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int oldScale = 9;

  @Test
  public void simpleTest() throws SFException {
    // test old and new dates
    long[] testSecondsInt64 = {1546391837, 1546391837, 0, 123, -12346, -12345};

    int[] testNanos = {0, 10, 100, 456, 876543211, 0};

    int[] testTimeZoneIndices = {960, 1440, 960, 960, 1440, 1440};

    String[] testTimesJson = {
      "1546391837.000000000 960",
      "1546391837.000000010 1440",
      "0.000000100 960",
      "123.000000456 960",
      "-12345.123456789 1440",
      "-12345.000000000 1440"
    };
    testTimestampTZ(testSecondsInt64, testNanos, testTimeZoneIndices, testTimesJson);
  }

  @Test
  public void timestampOverflowTest() throws SFException {
    // test old and new dates
    long[] testSecondsInt64 = {1546391837};

    int[] testNanos = {0};

    int[] testTimeZoneIndices = {960};

    String[] testTimesJson = {"1546391837.000000000 960"};
    testTimestampTZ(testSecondsInt64, testNanos, testTimeZoneIndices, testTimesJson);
  }

  public void testTimestampTZ(
      long[] testSecondsInt64, int[] testNanos, int[] testTimeZoneIndices, String[] testTimesJson)
      throws SFException {

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIMESTAMP");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    FieldType fieldType2 =
        new FieldType(true, Types.MinorType.INT.getType(), null, customFieldMeta);
    FieldType fieldType3 =
        new FieldType(true, Types.MinorType.INT.getType(), null, customFieldMeta);

    StructVector structVector = StructVector.empty("testVector", allocator);
    List<Field> fieldList = new LinkedList<Field>();
    Field bigIntField =
        new Field(ThreeFieldStructToTimestampTZConverter.FIELD_NAME_EPOCH, fieldType, null);
    Field fractionField =
        new Field(ThreeFieldStructToTimestampTZConverter.FIELD_NAME_FRACTION, fieldType2, null);
    Field timeZoneIndexField =
        new Field(
            ThreeFieldStructToTimestampTZConverter.FIELD_NAME_TIME_ZONE_INDEX, fieldType3, null);

    fieldList.add(bigIntField);
    fieldList.add(fractionField);
    fieldList.add(timeZoneIndexField);

    structVector.initializeChildrenFromFields(fieldList);
    BigIntVector seconds =
        structVector.getChild(
            ThreeFieldStructToTimestampTZConverter.FIELD_NAME_EPOCH, BigIntVector.class);
    IntVector nanos =
        structVector.getChild(
            ThreeFieldStructToTimestampTZConverter.FIELD_NAME_FRACTION, IntVector.class);
    IntVector timeZoneIdx =
        structVector.getChild(
            ThreeFieldStructToTimestampTZConverter.FIELD_NAME_TIME_ZONE_INDEX, IntVector.class);

    int i = 0, j = 0;
    while (i < testSecondsInt64.length) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        seconds.setNull(j);
        nanos.setNull(j);
        timeZoneIdx.setNull(j);
        nullValIndex.add(j);
      } else {
        seconds.setSafe(j, testSecondsInt64[i]);
        nanos.setSafe(j, testNanos[i]);
        timeZoneIdx.setSafe(j, testTimeZoneIndices[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter =
        new ThreeFieldStructToTimestampTZConverter(structVector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;
    this.setScale(testNanos[i]);
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
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBytes(x));
        i++;
        if (i < testNanos.length) {
          this.setScale(testNanos[i]);
        }
      }
      j++;
    }
    structVector.clear();
  }
}
