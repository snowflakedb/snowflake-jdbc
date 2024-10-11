/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.arrow;

import static net.snowflake.client.providers.ProvidersUtil.cartesianProduct;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.providers.SnowflakeArgumentsProvider;
import net.snowflake.client.providers.TimezoneProvider;
import net.snowflake.common.core.SFTimestamp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class BigIntToTimestampNTZConverterTest extends BaseConverterTest {
  static class FlagProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return Arrays.asList(Arguments.of(true), Arguments.of(false));
    }
  }

  static class DataProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return cartesianProduct(context, new TimezoneProvider(), new FlagProvider());
    }
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int oldScale = 9;

  @ParameterizedTest
  @ArgumentsSource(TimezoneProvider.class)
  public void testWithNullTimezone(String tz) throws SFException {
    System.setProperty("user.timezone", tz);
    testTimestampNTZ(null);
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testTimestampNTZ(String tz, boolean flag) throws SFException {
    this.setHonorClientTZForTimestampNTZ(flag);
    System.setProperty("user.timezone", tz);
    testTimestampNTZ(TimeZone.getDefault());
  }

  /**
   * Helper function for 2 tests above- can be tested with or without a timezone.
   *
   * @param timezone the timezone to be used for testing
   * @throws SFException
   */
  private void testTimestampNTZ(TimeZone timezone) throws SFException {
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

    ArrowVectorConverter converter = new BigIntToTimestampNTZConverter(vector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;
    this.setScale(testScales[i]);
    while (j < rowCount) {
      Timestamp ts = createTimestampObject(converter, j, timezone);
      Date date = converter.toDate(j, getTimeZone(), false);
      Time time = converter.toTime(j);
      String tsStr = converter.toString(j);

      if (tsStr != null) {
        assertFalse(converter.isNull(j));
      } else {
        assertTrue(converter.isNull(j));
      }

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
                java.sql.Types.TIMESTAMP,
                getResultVersion(),
                getTimeZone(),
                getSession());
        Timestamp oldTs = sfTimestamp.getTimestamp();
        if (getHonorClientTZForTimestampNTZ()) {
          // Note: honorClientTZForTimestampNTZ is used except getString()
          oldTs = sfTimestamp.moveToTimeZone(getTimeZone()).getTimestamp();
        }
        oldTs = ResultUtil.adjustTimestamp(oldTs);

        SFTimestamp sfTS =
            ResultUtil.getSFTimestamp(
                testTimesJson[0],
                oldScale,
                java.sql.Types.TIMESTAMP,
                getResultVersion(),
                getTimeZone(),
                getSession());
        String timestampStr =
            ResultUtil.getSFTimestampAsString(
                sfTS,
                java.sql.Types.TIMESTAMP,
                oldScale,
                getTimestampNTZFormatter(),
                getTimestampLTZFormatter(),
                getTimestampTZFormatter(),
                getSession());
        Date oldDate = new Date((oldTs).getTime());
        Time oldTime = new Time(oldTs.getTime());
        assertThat(oldTs, is(ts));
        assertThat(oldDate, is(date));
        assertThat(timestampStr, is(tsStr));
        assertThat(oldTime, is(time));
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

  private Timestamp createTimestampObject(ArrowVectorConverter converter, int j, TimeZone zone)
      throws SFException {
    return converter.toTimestamp(j, zone);
  }
}
