package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.json.DateTimeConverter;
import net.snowflake.client.providers.TimezoneProvider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

public class DateConverterTest extends BaseConverterTest {

  private static void setTimeZone(String tz) {
    System.setProperty("user.timezone", tz);
  }

  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  // test old and new dates
  int[] testDates = {-8865, -719162, -354285, -244712, -208156, -171664, -135107, 0, 16911};

  String[] expectedDates = {
    "1945-09-24",
    "0001-01-01",
    "1000-01-01",
    "1300-01-01",
    "1400-02-02",
    "1500-01-01",
    "1600-02-03",
    "1970-01-01",
    "2016-04-20"
  };

  // Map of timezone to ArrowResultUtil.getDate value and hours offset for testTimezoneDates test
  // case
  Map<String, List<Object>> timezoneDatesData =
      new HashMap<String, List<Object>>() {
        {
          put("UTC", Arrays.asList("2016-04-20", 0));
          put("America/Los_Angeles", Arrays.asList("2016-04-20", -7));
          put("America/New_York", Arrays.asList("2016-04-20", -4));
          put("Pacific/Honolulu", Arrays.asList("2016-04-20", -10));
          put("Asia/Singapore", Arrays.asList("2016-04-19", 8));
          put("CET", Arrays.asList("2016-04-19", 2)); // because of daylight savings
          put("GMT+0200", Arrays.asList("2016-04-19", 2));
        }
      };

  public static final int MILLIS_IN_ONE_HOUR = 3600000;
  private TimeZone defaultTimeZone;

  @BeforeEach
  public void getDefaultTimeZone() {
    this.defaultTimeZone = TimeZone.getDefault();
  }

  @AfterEach
  public void restoreDefaultTimeZone() {
    TimeZone.setDefault(defaultTimeZone);
  }

  @ParameterizedTest
  @ArgumentsSource(TimezoneProvider.class)
  public void testDate(String tz) throws SFException {
    setTimeZone(tz);
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.DATEDAY.getType(), null, customFieldMeta);

    DateDayVector vector = new DateDayVector("date", fieldType, allocator);
    int i = 0, j = 0;
    while (i < testDates.length) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(j);
        nullValIndex.add(j);
      } else {
        vector.setSafe(j, testDates[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter = new DateConverter(vector, 0, this, false);
    int rowCount = j;
    i = 0;
    j = 0;
    while (j < rowCount) {
      int intVal = converter.toInt(j);
      String strVal = converter.toString(j);
      Object obj = converter.toObject(j);
      if (strVal != null) {
        assertFalse(converter.isNull(j));
      } else {
        assertTrue(converter.isNull(j));
      }
      Object oldObj =
          ArrowResultUtil.getDate(intVal, TimeZone.getTimeZone("UTC"), TimeZone.getDefault());
      if (nullValIndex.contains(j)) {
        assertThat(intVal, is(0));
        assertThat(obj, is(nullValue()));
        assertThat(strVal, is(nullValue()));
        assertThat(false, is(converter.toBoolean(j)));
        assertThat(converter.toBytes(j), is(nullValue()));
      } else {
        assertThat(intVal, is(testDates[i]));
        assertThat(((Date) obj).getTime(), is(((Date) oldObj).getTime()));
        assertThat(obj.toString(), is(expectedDates[i]));
        assertThat(((Date) obj).getTime(), is(((Date) oldObj).getTime()));
        assertThat(oldObj.toString(), is(expectedDates[i++]));
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
      }
      j++;
    }
    vector.clear();
  }

  @ParameterizedTest
  @ArgumentsSource(TimezoneProvider.class)
  public void testRandomDates(String tz) throws SFException {
    setTimeZone(tz);
    int dateBound = 50000;
    int rowCount = 50000;
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.DATEDAY.getType(), null, customFieldMeta);

    DateDayVector vector = new DateDayVector("date", fieldType, allocator);
    int[] rawDates = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        nullValIndex.add(i);
        vector.setNull(i);
      } else {
        rawDates[i] = random.nextInt(dateBound) - dateBound / 2;
        vector.setSafe(i, rawDates[i]);
      }
    }

    ArrowVectorConverter converter = new DateConverter(vector, 0, this, false);

    for (int i = 0; i < rowCount; i++) {
      int intVal = converter.toInt(i);
      String strVal = converter.toString(i);
      Date obj = converter.toDate(i, getTimeZone(), false);
      String str = converter.toString(i);
      if (nullValIndex.contains(i)) {
        assertThat(intVal, is(0));
        assertThat(strVal, is(nullValue()));
        assertThat(obj, is(nullValue()));
      } else {
        Date oldObj = ArrowResultUtil.getDate(intVal);
        assertThat(intVal, is(rawDates[i]));
        assertThat(obj.getTime(), is(oldObj.getTime()));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(TimezoneProvider.class)
  public void testTimezoneDates(String tz) throws SFException {
    setTimeZone(tz);
    int testDay = 16911;
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    // test normal date
    FieldType fieldType =
        new FieldType(true, Types.MinorType.DATEDAY.getType(), null, customFieldMeta);

    DateDayVector vector = new DateDayVector("date", fieldType, allocator);

    vector.setSafe(0, testDay);

    // Test JDBC_FORMAT_DATE_WITH_TIMEZONE=TRUE with different session timezones
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    ArrowVectorConverter converter = new DateConverter(vector, 0, this, true);
    converter.setUseSessionTimezone(true);
    converter.setSessionTimeZone(TimeZone.getTimeZone(tz));
    Object obj = converter.toObject(0);
    DateTimeConverter jsonConverter =
        new DateTimeConverter(
            TimeZone.getTimeZone(tz), this.getSession(), 0, true, false, true, true);
    Date jsonDate =
        jsonConverter.getDate(Integer.toString(testDay), 91, 91, TimeZone.getTimeZone(tz), 0);
    Object utcObj =
        ArrowResultUtil.getDate(testDay, TimeZone.getTimeZone("UTC"), TimeZone.getTimeZone(tz));

    List<Object> testValues = this.timezoneDatesData.get(tz);
    assertTrue(testValues.get(0) instanceof String);
    assertTrue(testValues.get(1) instanceof Integer);
    assertThat(obj.toString(), is("2016-04-20"));
    assertThat(jsonDate.toString(), is(obj.toString()));
    assertThat(utcObj.toString(), is(testValues.get(0)));
    assertThat(
        ((Date) obj).getTime(),
        is(((Date) utcObj).getTime() + ((Integer) testValues.get(1) * MILLIS_IN_ONE_HOUR)));

    vector.clear();
  }
}
