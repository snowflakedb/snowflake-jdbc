package net.snowflake.client.core.arrow;

import static java.util.stream.Stream.concat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Stream;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.common.core.SFTimestamp;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class TwoFieldStructToTimestampNTZConverterTest extends BaseConverterTest {

  private static void setTimezone(String tz) {
    System.setProperty("user.timezone", tz);
  }

  @AfterAll
  public static void clearTimezone() {
    System.clearProperty("user.timezone");
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  private int oldScale = 9;

  static class DataProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      List<String> timezones =
          new ArrayList<String>() {
            {
              add("America/Los_Angeles");
              add("America/New_York");
              add("Pacific/Honolulu");
              add("Asia/Singapore");
              add("MESZ");
              add("MEZ");
              add("UTC");
            }
          };

      Stream<Arguments> args = Stream.empty();

      for (String timezone : timezones) {
        args =
            concat(
                args,
                Stream.of(
                    Arguments.argumentSet(
                        timezone + " Overflow",
                        timezone,
                        false,
                        new long[] {154639183700000L},
                        new int[] {0},
                        new String[] {"154639183700000.000000000"}),
                    Arguments.argumentSet(
                        timezone + " HonorClientTZForTimestampNTZ Disabled",
                        timezone,
                        false,
                        new long[] {1546391837, 0, -1546391838, -1546391838, -1546391838},
                        new int[] {0, 1, 999999990, 876543211, 1},
                        new String[] {
                          "1546391837.000000000",
                          "0.000000001",
                          "-1546391837.000000010",
                          "-1546391837.123456789",
                          "-1546391837.999999999"
                        }),
                    Arguments.argumentSet(
                        timezone + " HonorClientTZForTimestampNTZ Enabled",
                        timezone,
                        true,
                        new long[] {1546391837, 1546391837, 1546391837, 1546391837, 1546391837},
                        new int[] {0, 1, 10, 100, 999999999},
                        new String[] {
                          "1546391837.000000000",
                          "1546391837.000000001",
                          "1546391837.000000010",
                          "1546391837.000000100",
                          "1546391837.999999999"
                        })));
      }

      return args;
    }
  }

  @ParameterizedTest
  @ArgumentsSource(DataProvider.class)
  public void testTimestampNTZ(
      String timezone,
      boolean honorClientTZForTimestampNTZ,
      long[] testSecondsInt64,
      int[] testNanoSecs,
      String[] testTimesJson)
      throws SFException {
    this.setHonorClientTZForTimestampNTZ(honorClientTZForTimestampNTZ);
    setTimezone(timezone);

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "TIMESTAMP");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);
    FieldType fieldType2 =
        new FieldType(true, Types.MinorType.INT.getType(), null, customFieldMeta);

    StructVector structVector = StructVector.empty("testListVector", allocator);
    List<Field> fieldList = new LinkedList<Field>();
    Field bigIntField =
        new Field(TwoFieldStructToTimestampNTZConverter.FIELD_NAME_EPOCH, fieldType, null);

    Field intField =
        new Field(TwoFieldStructToTimestampNTZConverter.FIELD_NAME_FRACTION, fieldType2, null);

    fieldList.add(bigIntField);
    fieldList.add(intField);

    structVector.initializeChildrenFromFields(fieldList);
    BigIntVector epochs =
        structVector.getChild(
            TwoFieldStructToTimestampNTZConverter.FIELD_NAME_EPOCH, BigIntVector.class);

    IntVector fractions =
        structVector.getChild(
            TwoFieldStructToTimestampNTZConverter.FIELD_NAME_FRACTION, IntVector.class);

    int i = 0, j = 0;
    while (i < testSecondsInt64.length) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        epochs.setNull(j);
        fractions.setNull(j);
        nullValIndex.add(j);
      } else {
        epochs.setSafe(j, testSecondsInt64[i]);
        fractions.setSafe(j, testNanoSecs[i++]);
        structVector.setIndexDefined(j);
      }
      j++;
    }
    structVector.setValueCount(j);

    ArrowVectorConverter converter =
        new TwoFieldStructToTimestampNTZConverter(structVector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;

    while (j < rowCount) {
      Timestamp ts = converter.toTimestamp(j, TimeZone.getDefault());
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
                testTimesJson[i],
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
                testTimesJson[i],
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
        i++;
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBytes(x));
      }
      j++;
    }
    structVector.clear();
  }
}
