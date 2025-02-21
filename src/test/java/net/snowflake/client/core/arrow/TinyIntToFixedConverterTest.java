package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
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
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class TinyIntToFixedConverterTest extends BaseConverterTest {
  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** Random seed */
  private Random random = new Random();

  @Test
  public void testFixedNoScale() throws SFException {
    final int rowCount = 1000;
    List<Byte> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add((byte) random.nextInt(1 << 8));
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "0");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    TinyIntVector vector = new TinyIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new TinyIntToFixedConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++) {
      byte byteVal = converter.toByte(i);
      Object longObject = converter.toObject(i); // the logical type is long
      String byteString = converter.toString(i);

      if (nullValIndex.contains(i)) {
        assertThat(byteVal, is((byte) 0));
        assertThat(longObject, is(nullValue()));
        assertThat(byteString, is(nullValue()));
      } else {
        assertThat(byteVal, is(expectedValues.get(i)));
        assertEquals(longObject, (long) expectedValues.get(i));
        assertThat(byteString, is(expectedValues.get(i).toString()));
      }
    }
    vector.clear();
  }

  @Test
  public void testFixedWithScale() throws SFException {
    final int rowCount = 1000;
    List<Byte> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add((byte) random.nextInt(1 << 8));
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "FIXED");
    customFieldMeta.put("precision", "10");
    customFieldMeta.put("scale", "1");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    TinyIntVector vector = new TinyIntVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new TinyIntToScaledFixedConverter(vector, 0, this, 1);

    for (int i = 0; i < rowCount; i++) {
      BigDecimal bigDecimalVal = converter.toBigDecimal(i);
      Object objectVal = converter.toObject(i);
      String stringVal = converter.toString(i);

      if (nullValIndex.contains(i)) {
        assertThat(bigDecimalVal, nullValue());
        assertThat(objectVal, nullValue());
        assertThat(stringVal, nullValue());
      } else {
        BigDecimal expectedVal = BigDecimal.valueOf(expectedValues.get(i), 1);
        assertThat(bigDecimalVal, is(expectedVal));
        assertThat(objectVal, is(expectedVal));
        assertThat(stringVal, is(expectedVal.toString()));
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
    customFieldMeta.put("scale", "1");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    TinyIntVector vector = new TinyIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 200);

    final ArrowVectorConverter converter = new TinyIntToScaledFixedConverter(vector, 0, this, 1);
    final int invalidConversionErrorCode = ErrorCode.INVALID_VALUE_CONVERT.getMessageCode();

    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toLong(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toInt(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toShort(0));
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
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    // test value which is in range of byte, all get method should return
    TinyIntVector vectorBar = new TinyIntVector("col_one", fieldType, allocator);
    // set value which is out of range of int, but falls in long
    vectorBar.setSafe(0, 10);
    vectorBar.setSafe(1, -10);

    final ArrowVectorConverter converterBar = new TinyIntToFixedConverter(vectorBar, 0, this);

    assertThat(converterBar.toShort(0), is((short) 10));
    assertThat(converterBar.toShort(1), is((short) -10));
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
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    TinyIntVector vector = new TinyIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    ArrowVectorConverter converter = new TinyIntToFixedConverter(vector, 0, this);

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
        new FieldType(true, Types.MinorType.TINYINT.getType(), null, customFieldMeta);

    TinyIntVector vector = new TinyIntVector("col_one", fieldType, allocator);
    vector.setSafe(0, 0);
    vector.setSafe(1, 1);
    vector.setNull(2);
    vector.setSafe(3, 5);

    final ArrowVectorConverter converter = new TinyIntToScaledFixedConverter(vector, 0, this, 3);

    assertFalse(converter.toBoolean(0));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));
    assertFalse(converter.toBoolean(2));
    TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(3));

    assertFalse(converter.isNull(0));
    assertFalse(converter.isNull(1));
    assertTrue(converter.isNull(2));
    assertFalse(converter.isNull(3));
    vector.close();
  }
}
