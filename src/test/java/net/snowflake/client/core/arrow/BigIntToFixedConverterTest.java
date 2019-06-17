/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.TestRunInterface;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class BigIntToFixedConverterTest
{
  /**
   * allocator for arrow
   */
  private BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  /**
   * Random seed
   */
  private Random random = new Random();

  @Test
  public void testFixedNoScale() throws SFException
  {
    final int rowCount = 1000;
    List<Long> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++)
    {
      expectedValues.add(random.nextLong());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.BIGINT.getType(),
                                        null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++)
    {
      boolean isNull = random.nextBoolean();
      if (isNull)
      {
        vector.setNull(i);
        nullValIndex.add(i);
      }
      else
      {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new BigIntToFixedConverter(vector);

    for (int i = 0; i < rowCount; i++)
    {
      long longVal = converter.toLong(i);
      Object longObject = converter.toObject(i);
      String longString = converter.toString(i);

      if (nullValIndex.contains(i))
      {
        assertThat(longVal, is(0L));
        assertThat(longObject, is(nullValue()));
        assertThat(longString, is(nullValue()));
      }
      else
      {
        assertThat(longVal, is(expectedValues.get(i)));
        assertThat(longObject, is(expectedValues.get(i)));
        assertThat(longString, is(expectedValues.get(i).toString()));
      }
    }
    vector.clear();
  }

  @Test
  public void testFixedWithScale() throws SFException
  {
    final int rowCount = 1000;
    List<Long> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++)
    {
      expectedValues.add(random.nextLong());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.BIGINT.getType(),
                                        null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++)
    {
      boolean isNull = random.nextBoolean();
      if (isNull)
      {
        vector.setNull(i);
        nullValIndex.add(i);
      }
      else
      {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new BigIntToFixedConverter(vector);

    for (int i = 0; i < rowCount; i++)
    {
      BigDecimal bigDecimalVal = converter.toBigDecimal(i);
      BigDecimal bigDecimalWithScaleVal = converter.toBigDecimal(i, 1);
      Object objectVal = converter.toObject(i);
      String stringVal = converter.toString(i);

      if (nullValIndex.contains(i))
      {
        assertThat(bigDecimalVal, nullValue());
        assertThat(bigDecimalWithScaleVal, nullValue());
        assertThat(objectVal, nullValue());
        assertThat(stringVal, nullValue());
      }
      else
      {
        BigDecimal expectedVal = BigDecimal.valueOf(expectedValues.get(i), 3);
        BigDecimal expectedWithScaleVal = expectedVal.setScale(1,
                                                               RoundingMode.HALF_UP);
        assertThat(bigDecimalVal, is(expectedVal));
        assertThat(bigDecimalWithScaleVal, is(expectedWithScaleVal));
        assertThat(objectVal, is(expectedVal));
        assertThat(stringVal, is(expectedVal.toString()));
      }
    }

    vector.clear();
  }

  @Test
  public void testInvalidConversion()
  {
    // try convert to int/long/byte/short with scale > 0
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.BIGINT.getType(),
                                        null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 123456789L);

    final ArrowVectorConverter converter = new BigIntToFixedConverter(vector);
    assertInvalidConversion(() -> converter.toDouble(0));
    assertInvalidConversion(() -> converter.toLong(0));
    assertInvalidConversion(() -> converter.toInt(0));
    assertInvalidConversion(() -> converter.toShort(0));
    assertInvalidConversion(() -> converter.toByte(0));
    vector.clear();
  }

  @Test
  public void testGetSmallerIntegralType() throws SFException
  {
    // try convert to int/long/byte/short with scale > 0
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.BIGINT.getType(),
                                        null, customFieldMeta);

    // test value which is out of range of int, but falls in long
    BigIntVector vectorFoo = new BigIntVector("col_one", fieldType, allocator);
    vectorFoo.setSafe(0, 2147483650L);
    vectorFoo.setSafe(1, -2147483650L);

    final ArrowVectorConverter converterFoo =
        new BigIntToFixedConverter(vectorFoo);
    assertInvalidConversion(() -> converterFoo.toInt(0));
    assertInvalidConversion(() -> converterFoo.toShort(0));
    assertInvalidConversion(() -> converterFoo.toByte(0));
    assertInvalidConversion(() -> converterFoo.toInt(1));
    assertInvalidConversion(() -> converterFoo.toShort(1));
    assertInvalidConversion(() -> converterFoo.toByte(1));
    vectorFoo.clear();

    // test value which is in range of byte, all get method should return
    BigIntVector vectorBar = new BigIntVector("col_one", fieldType, allocator);
    // set value which is out of range of int, but falls in long
    vectorBar.setSafe(0, 10L);
    vectorBar.setSafe(1, -10L);

    final ArrowVectorConverter converterBar =
        new BigIntToFixedConverter(vectorBar);

    assertThat(converterBar.toByte(0), is((byte) 10));
    assertThat(converterBar.toByte(1), is((byte) -10));
    assertThat(converterBar.toShort(0), is((short) 10));
    assertThat(converterBar.toShort(1), is((short) -10));
    assertThat(converterBar.toInt(0), is(10));
    assertThat(converterBar.toInt(1), is(-10));
    vectorBar.clear();
  }

  /**
   * util functions to assert exception when running a piece of test code
   */
  private void assertInvalidConversion(TestRunInterface testRunInterface)
  {
    try
    {
      testRunInterface.run();
      Assert.fail();
    }
    catch (SFException e)
    {
      assertThat(e.getVendorCode(), is(
          ErrorCode.INVALID_VALUE_CONVERT.getMessageCode()));
    }
  }
}
