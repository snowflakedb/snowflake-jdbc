package net.snowflake.client.core.arrow;

import net.snowflake.client.core.ResultUtil;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SFSession;
import net.snowflake.common.core.SnowflakeDateTimeFormat;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Test;

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


public class IntToDateConverterTest
{
  /**
   * allocator for arrow
   */
  private BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  private Random random = new Random();

  private SnowflakeDateTimeFormat df = new SnowflakeDateTimeFormat("YYYY-MM-DD");
  // test old and new dates
  int[] testDates = {
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
      "0001-01-01",
      "1000-01-01",
      "1300-01-01",
      "1400-02-02",
      "1500-01-01",
      "1600-02-03",
      "1970-01-01",
      "2016-04-20"
  };

//  @Test
  public void testDate() throws SFException
  {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.INT.getType(),
                                        null, customFieldMeta);

    IntVector vector = new IntVector("date", fieldType, allocator);
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

    ArrowVectorConverter converter = new IntToDateConverter(vector, df);
    int rowCount = j;
    i = 0;
    j = 0;
    while (j < rowCount)
    {
      int intVal = converter.toInt(j);
      String strVal = converter.toString(j);
      Object obj = converter.toObject(j);
      Object oldObj = ResultUtil.getDate(Integer.toString(intVal), TimeZone.getDefault(), new SFSession());
      if (nullValIndex.contains(j))
      {
        assertThat(intVal, is(0));
        assertThat(obj, is(nullValue()));
        assertThat(strVal, is(nullValue()));
      }
      else
      {
        assertThat(intVal, is(testDates[i]));
        assertThat(((Date)obj).getTime(), is(((Date) oldObj).getTime()));
        assertThat(strVal, is(expectedDates[i]));
        assertThat(obj.toString(), is(expectedDates[i++]));
      }
      j++;
    }
    vector.clear();
  }

//  @Test
  public void testRandomDates() throws SFException
  {
    int dateBound = 50000;
    int rowCount = 10000;
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DATE");
    Set<Integer> nullValIndex = new HashSet<>();
    // test normal date
    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.INT.getType(),
                                        null, customFieldMeta);

    IntVector vector = new IntVector("date", fieldType, allocator);
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
        rawDates[i] = random.nextInt(dateBound) - dateBound/2;
        vector.setSafe(i, rawDates[i]);
      }
    }

    ArrowVectorConverter converter = new IntToDateConverter(vector, df);

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
        Date oldObj = ResultUtil.getDate(Integer.toString(intVal), TimeZone.getDefault(), new SFSession());
        String oldStr = ResultUtil.getDateAsString(oldObj, df);
        assertThat(intVal, is(rawDates[i]));
        assertThat(obj.getTime(), is(oldObj.getTime()));
        assertThat(str, is(oldStr));
      }
    }
  }
}
