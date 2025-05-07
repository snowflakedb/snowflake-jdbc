package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class BigIntToFixedConverterTest extends BaseConverterTest {
  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** Random seed */
  private Random random = new Random();

  private ByteBuffer bb;

  @Test
  public void testFixedNoScale() throws SFException {
    final int rowCount = 1000;
    List<Long> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add(random.nextLong());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new BigIntToFixedConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++) {
      long longVal = converter.toLong(i);
      Object longObject = converter.toObject(i);
      String longString = converter.toString(i);

      if (longString != null) {
        assertFalse(converter.isNull(i));
      } else {
        assertTrue(converter.isNull(i));
      }

      if (nullValIndex.contains(i)) {
        assertThat(longVal, is(0L));
        assertThat(longObject, is(nullValue()));
        assertThat(longString, is(nullValue()));
        assertThat(converter.toBytes(i), is(nullValue()));
      } else {
        assertThat(longVal, is(expectedValues.get(i)));
        assertThat(longObject, is(expectedValues.get(i)));
        assertThat(longString, is(expectedValues.get(i).toString()));
        bb = ByteBuffer.wrap(converter.toBytes(i));
        assertThat(longVal, is(bb.getLong()));
      }
    }
    vector.clear();
  }

  @Test
  public void testFixedWithScale() throws SFException {
    final int rowCount = 1000;
    List<Long> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add(random.nextLong());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new BigIntToScaledFixedConverter(vector, 0, this, 3);

    for (int i = 0; i < rowCount; i++) {
      BigDecimal bigDecimalVal = converter.toBigDecimal(i);
      Object objectVal = converter.toObject(i);
      String stringVal = converter.toString(i);

      if (nullValIndex.contains(i)) {
        assertThat(bigDecimalVal, nullValue());
        assertThat(objectVal, nullValue());
        assertThat(stringVal, nullValue());
        assertThat(converter.toBytes(i), is(nullValue()));
      } else {
        BigDecimal expectedVal = BigDecimal.valueOf(expectedValues.get(i), 3);
        assertThat(bigDecimalVal, is(expectedVal));
        assertThat(objectVal, is(expectedVal));
        assertThat(stringVal, is(expectedVal.toString()));
        assertThat(converter.toBytes(i), is(notNullValue()));
      }
    }

    vector.clear();
  }

  @Test
  public void testInvalidConversion() {
    // try convert to int/long/byte/short with scale > 0
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 123456789L);

    final ArrowVectorConverter converter = new BigIntToScaledFixedConverter(vector, 0, this, 3);

    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toLong(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toInt(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toShort(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toByte(0));
    TestUtil.assertSFException(
        invalidConversionErrorCode, () -> converter.toDate(0, getTimeZone(), false));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toTime(0));
    TestUtil.assertSFException(
        invalidConversionErrorCode, () -> converter.toTimestamp(0, TimeZone.getDefault()));
    vector.clear();
  }

  @Test
  public void testGetSmallerIntegralType() throws SFException {
    // try convert to int/long/byte/short with scale > 0
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    // test value which is out of range of int, but falls in long
    BigIntVector vectorFoo = new BigIntVector("col_one", fieldType, allocator);
    vectorFoo.setSafe(0, 2147483650L);
    vectorFoo.setSafe(1, -2147483650L);

    final ArrowVectorConverter converterFoo = new BigIntToFixedConverter(vectorFoo, 0, this);

    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toInt(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toShort(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toByte(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toInt(1));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toShort(1));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toByte(1));
    vectorFoo.clear();

    // test value which is in range of byte, all get method should return
    BigIntVector vectorBar = new BigIntVector("col_one", fieldType, allocator);
    // set value which is out of range of int, but falls in long
    vectorBar.setSafe(0, 10L);
    vectorBar.setSafe(1, -10L);

    final ArrowVectorConverter converterBar = new BigIntToFixedConverter(vectorBar, 0, this);

    assertThat(converterBar.toByte(0), is((byte) 10));
    assertThat(converterBar.toByte(1), is((byte) -10));
    assertThat(converterBar.toShort(0), is((short) 10));
    assertThat(converterBar.toShort(1), is((short) -10));
    assertThat(converterBar.toInt(0), is(10));
    assertThat(converterBar.toInt(1), is(-10));
    vectorBar.clear();
  }

  @Test
  public void testGetBooleanNoScale() throws SFException {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    ArrowVectorConverter converter = new BigIntToFixedConverter(vector, 0, this);

    assertThat(false, is(converter.toBoolean(0)));
    assertThat(true, is(converter.toBoolean(1)));
    assertThat(false, is(converter.toBoolean(2)));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));

    vector.close();
  }

  @Test
  public void testGetBooleanWithScale() throws SFException {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.BIGINT.getType(), null, customFieldMeta);

    BigIntVector vector = new BigIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    final ArrowVectorConverter converter = new BigIntToScaledFixedConverter(vector, 0, this, 3);

    assertThat(false, is(converter.toBoolean(0)));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));
    assertThat(false, is(converter.toBoolean(2)));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));

    vector.close();
  }
}
