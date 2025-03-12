package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class DoubleToRealConverterTest extends BaseConverterTest {
  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  /** Random seed */
  private Random random = new Random();

  private ByteBuffer bb;

  @Test
  public void testConvertToDouble() throws SFException {
    final int rowCount = 1000;
    List<Double> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add(random.nextDouble());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "REAL");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.FLOAT8.getType(), null, customFieldMeta);

    Float8Vector vector = new Float8Vector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter =
        new DoubleToRealConverter(vector, 0, (DataConversionContext) this);

    for (int i = 0; i < rowCount; i++) {
      double doubleVal = converter.toDouble(i);
      float floatVal = converter.toFloat(i);
      Object doubleObject = converter.toObject(i);
      String doubleString = converter.toString(i);
      if (doubleObject != null) {
        assertFalse(converter.isNull(i));
      } else {
        assertTrue(converter.isNull(i));
      }

      if (nullValIndex.contains(i)) {
        assertThat(doubleVal, is((double) 0));
        assertThat(floatVal, is((float) 0));
        assertThat(doubleObject, is(nullValue()));
        assertThat(doubleString, is(nullValue()));
        assertThat(false, is(converter.toBoolean(i)));
        assertThat(converter.toBytes(i), is(nullValue()));
      } else {
        assertThat(doubleVal, is(expectedValues.get(i)));
        assertThat(floatVal, is(expectedValues.get(i).floatValue()));
        assertThat(doubleObject, is(expectedValues.get(i)));
        assertThat(doubleString, is(expectedValues.get(i).toString()));
        final int x = i;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
        bb = ByteBuffer.wrap(converter.toBytes(i));
        assertThat(doubleVal, is(bb.getDouble()));
      }
    }
    vector.clear();
  }
}
