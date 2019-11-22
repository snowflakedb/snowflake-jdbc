package net.snowflake.client.core.arrow;

import net.snowflake.client.TestUtil;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
public class DateConverterTest extends BaseConverterTest
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

  public DateConverterTest(String tz)
  {
    System.setProperty("user.timezone", tz);
  }

  /**
   * allocator for arrow
   */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  // test old and new dates
  int[] testDates = {
      -8865,
      -719162,
      -354285,
      -244712,
      -208156,
      -171664,
      -135107,
      0,
      16911
  };

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

  @Test
  public void testDate() throws SFException
  {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.DATEDAY.getType(),
                                        null, customFieldMeta);

    DateDayVector vector = new DateDayVector("date", fieldType, allocator);
    int i = 0, j = 0;
    while (i < testDates.length)
    {
      boolean isNull = random.nextBoolean();
      if (isNull)
      {
        vector.setNull(j);
        nullValIndex.add(j);
      }
      else
      {
        vector.setSafe(j, testDates[i++]);
      }
      j++;
    }

    ArrowVectorConverter converter = new DateConverter(vector, 0, this);
    int rowCount = j;
    i = 0;
    j = 0;
    while (j < rowCount)
    {
      int intVal = converter.toInt(j);
      String strVal = converter.toString(j);
      Object obj = converter.toObject(j);
      Object oldObj = ArrowResultUtil.getDate(intVal, TimeZone.getDefault(), new SFSession());
      if (nullValIndex.contains(j))
      {
        assertThat(intVal, is(0));
        assertThat(obj, is(nullValue()));
        assertThat(strVal, is(nullValue()));
        assertThat(false, is(converter.toBoolean(j)));
        assertThat(converter.toBytes(j), is(nullValue()));
      }
      else
      {
        assertThat(intVal, is(testDates[i]));
        assertThat(((Date) obj).getTime(), is(((Date) oldObj).getTime()));
        assertThat(obj.toString(), is(expectedDates[i]));
        assertThat(((Date) obj).getTime(), is(((Date) oldObj).getTime()));
        assertThat(oldObj.toString(), is(expectedDates[i++]));
        final int x = j;
        TestUtil.assertSFException(invalidConversionErrorCode,
                                   () -> converter.toBoolean(x));
      }
      j++;
    }
    vector.clear();
  }

  @Test
  public void testRandomDates() throws SFException
  {
    int dateBound = 50000;
    int rowCount = 50000;
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.DATEDAY.getType(),
                                        null, customFieldMeta);

    DateDayVector vector = new DateDayVector("date", fieldType, allocator);
    int[] rawDates = new int[rowCount];
    for (int i = 0; i < rowCount; i++)
    {
      boolean isNull = random.nextBoolean();
      if (isNull)
      {
        nullValIndex.add(i);
        vector.setNull(i);
      }
      else
      {
        rawDates[i] = random.nextInt(dateBound) - dateBound / 2;
        vector.setSafe(i, rawDates[i]);
      }
    }

    ArrowVectorConverter converter = new DateConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++)
    {
      int intVal = converter.toInt(i);
      String strVal = converter.toString(i);
      Date obj = converter.toDate(i);
      String str = converter.toString(i);
      if (nullValIndex.contains(i))
      {
        assertThat(intVal, is(0));
        assertThat(strVal, is(nullValue()));
        assertThat(obj, is(nullValue()));
      }
      else
      {
        Date oldObj = ArrowResultUtil.getDate(intVal);
        assertThat(intVal, is(rawDates[i]));
        assertThat(obj.getTime(), is(oldObj.getTime()));
      }
    }
  }
}
