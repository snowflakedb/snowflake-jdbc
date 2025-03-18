package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import net.snowflake.client.jdbc.ErrorCode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class SmallIntToFixedConverterTest extends BaseConverterTest {
  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** Random seed */
  private Random random = new Random();

  private ByteBuffer bb;

  @Test
  public void testFixedNoScale() throws SFException {
    final int rowCount = 1000;
    List<Short> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add((short) random.nextInt(1 << 16));
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    SmallIntVector vector = new SmallIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new SmallIntToFixedConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++) {
      short shortVal = converter.toShort(i);
      Object longObject = converter.toObject(i); // the logical type is long
      String shortString = converter.toString(i);
      if (shortString != null) {
        assertFalse(converter.isNull(i));
      } else {
        assertTrue(converter.isNull(i));
      }

      if (nullValIndex.contains(i)) {
        assertThat(shortVal, is((short) 0));
        assertThat(longObject, is(nullValue()));
        assertThat(shortString, is(nullValue()));
        assertThat(converter.toBytes(i), is(nullValue()));
      } else {
        assertThat(shortVal, is(expectedValues.get(i)));
        assertEquals(longObject, (long) expectedValues.get(i));
        assertThat(shortString, is(expectedValues.get(i).toString()));
        bb = ByteBuffer.wrap(converter.toBytes(i));
        assertThat(shortVal, is(bb.getShort()));
      }
    }
    vector.clear();
  }

  @Test
  public void testFixedWithScale() throws SFException {
    final int rowCount = 1000;
    List<Short> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add((short) random.nextInt(1 << 16));
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "3");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    SmallIntVector vector = new SmallIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new SmallIntToScaledFixedConverter(vector, 0, this, 3);

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
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    SmallIntVector vector = new SmallIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 200);

    final ArrowVectorConverter converter = new SmallIntToScaledFixedConverter(vector, 0, this, 3);
    final int invalidConversionErrorCode = ErrorCode.INVALID_VALUE_CONVERT.getMessageCode();

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
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    // test value which is out of range of int, but falls in long
    SmallIntVector vectorFoo = new SmallIntVector("col_one", fieldType, allocator);
    vectorFoo.setSafe(0, 200);
    vectorFoo.setSafe(1, -200);

    final ArrowVectorConverter converterFoo = new SmallIntToFixedConverter(vectorFoo, 0, this);
    final int invalidConversionErrorCode = ErrorCode.INVALID_VALUE_CONVERT.getMessageCode();

    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toByte(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converterFoo.toByte(1));
    vectorFoo.clear();

    // test value which is in range of byte, all get method should return
    SmallIntVector vectorBar = new SmallIntVector("col_one", fieldType, allocator);
    // set value which is out of range of int, but falls in long
    vectorBar.setSafe(0, 10);
    vectorBar.setSafe(1, -10);

    final ArrowVectorConverter converterBar = new SmallIntToFixedConverter(vectorBar, 0, this);

    assertThat(converterBar.toByte(0), is((byte) 10));
    assertThat(converterBar.toByte(1), is((byte) -10));
    assertThat(converterBar.toInt(0), is(10));
    assertThat(converterBar.toInt(1), is(-10));
    assertThat(converterBar.toLong(0), is(10L));
    assertThat(converterBar.toLong(1), is(-10L));
    vectorBar.clear();
  }

  @Test
  public void testGetBooleanNoScale() throws SFException {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    SmallIntVector vector = new SmallIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    ArrowVectorConverter converter = new SmallIntToFixedConverter(vector, 0, this);

    assertThat(false, is(converter.toBoolean(0)));
    assertThat(true, is(converter.toBoolean(1)));
    assertThat(false, is(converter.toBoolean(2)));
    assertFalse(converter.isNull(0));
    assertFalse(converter.isNull(1));
    assertTrue(converter.isNull(2));
    assertFalse(converter.isNull(3));
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
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    SmallIntVector vector = new SmallIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    final ArrowVectorConverter converter = new SmallIntToScaledFixedConverter(vector, 0, this, 3);

    assertThat(false, is(converter.toBoolean(0)));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));
    assertThat(false, is(converter.toBoolean(2)));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));

    vector.close();
  }
}
