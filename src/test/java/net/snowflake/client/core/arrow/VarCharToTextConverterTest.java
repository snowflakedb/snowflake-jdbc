/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
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

public class VarCharToTextConverterTest extends BaseConverterTest
{
  /**
   * allocator for arrow
   */
  private BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

  private Random random = new Random();

  @Test
  public void testConvertToString() throws SFException
  {
    final int rowCount = 1000;
    List<String> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++)
    {
      expectedValues.add(RandomStringUtils.random(20));
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");

    FieldType fieldType = new FieldType(true,
                                        Types.MinorType.VARCHAR.getType(),
                                        null, customFieldMeta);

    VarCharVector vector = new VarCharVector("col_one", fieldType,
                                             allocator);
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
        vector.setSafe(i, expectedValues.get(i).getBytes(StandardCharsets.UTF_8));
      }
    }

    ArrowVectorConverter converter = new VarCharToTextConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++)
    {
      String stringVal = converter.toString(i);
      Object objectVal = converter.toObject(i);
      byte[] bytesVal = converter.toBytes(i);

      if (nullValIndex.contains(i))
      {
        assertThat(stringVal, is(nullValue()));
        assertThat(objectVal, is(nullValue()));
        assertThat(bytesVal, is(nullValue()));
      }
      else
      {
        assertThat(stringVal, is(expectedValues.get(i)));
        assertThat(objectVal, is(expectedValues.get(i)));
        assertThat(bytesVal,
                   is(expectedValues.get(i).getBytes(StandardCharsets.UTF_8)));
      }
    }
    vector.clear();
  }
}
