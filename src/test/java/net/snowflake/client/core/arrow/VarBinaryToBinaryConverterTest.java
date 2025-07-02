package net.snowflake.client.core.arrow;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import net.snowflake.client.TestUtil;
import net.snowflake.client.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

public class VarBinaryToBinaryConverterTest extends BaseConverterTest {
  /** allocator for arrow */
  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  private Random random = new Random();

  @Test
  public void testConvertToString() throws SFException {
    final int rowCount = 1000;
    List<byte[]> expectedValues = new ArrayList<>();
    Set<Integer> nullValIndex = new HashSet<>();
    for (int i = 0; i < rowCount; i++) {
      expectedValues.add(RandomStringUtils.random(20).getBytes());
    }

    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "BINARY");

    FieldType fieldType =
        new FieldType(true, Types.MinorType.VARBINARY.getType(), null, customFieldMeta);

    VarBinaryVector vector = new VarBinaryVector("col_one", fieldType, allocator);
    for (int i = 0; i < rowCount; i++) {
      boolean isNull = random.nextBoolean();
      if (isNull) {
        vector.setNull(i);
        nullValIndex.add(i);
      } else {
        vector.setSafe(i, expectedValues.get(i));
      }
    }

    ArrowVectorConverter converter = new VarBinaryToBinaryConverter(vector, 0, this);

    for (int i = 0; i < rowCount; i++) {
      String stringVal = converter.toString(i);
      Object objectVal = converter.toObject(i);
      byte[] bytesVal = converter.toBytes(i);
      if (stringVal != null) {
        assertFalse(converter.isNull(i));
      } else {
        assertTrue(converter.isNull(i));
      }

      if (nullValIndex.contains(i)) {
        assertThat(stringVal, is(nullValue()));
        assertThat(objectVal, is(nullValue()));
        assertThat(bytesVal, is(nullValue()));
        assertThat(false, is(converter.toBoolean(i)));
      } else {
        String base64Expected = Base64.getEncoder().encodeToString(expectedValues.get(i));
        assertThat(stringVal, is(base64Expected));
        assertThat(bytesVal, is(expectedValues.get(i)));
        assertThat(objectVal, is(expectedValues.get(i)));
        final int x = i;
        TestUtil.assertSFException(invalidConversionErrorCode, () -> converter.toBoolean(x));
      }
    }
    vector.clear();
  }
}
