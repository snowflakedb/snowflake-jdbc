package net.snowflake.client.internal.core.arrow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.snowflake.client.internal.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class DecfloatToDecimalConverterTest extends BaseConverterTest {

  private BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private StructVector structVector;

  @AfterEach
  public void closeVector() {
    if (structVector != null) {
      structVector.close();
    }
    allocator.close();
  }

  @Test
  public void testToStringUsesCanonicalScientificNotation() throws SFException {
    BigDecimal scientificValue = new BigDecimal("1.23E-7");
    BigDecimal plainValue = new BigDecimal("123.456");

    structVector = createDecfloatVector(scientificValue, plainValue, null);
    ArrowVectorConverter converter = new DecfloatToDecimalConverter(structVector, 0, this);

    assertEquals("1.23E-7", converter.toString(0));
    assertEquals("123E-9", scientificValue.toEngineeringString());
    assertEquals("123.456", converter.toString(1));
    assertNull(converter.toString(2));
    assertEquals(scientificValue, converter.toBigDecimal(0));
    assertEquals(plainValue, converter.toBigDecimal(1));
    assertNull(converter.toBigDecimal(2));
  }

  private StructVector createDecfloatVector(BigDecimal... values) {
    Map<String, String> customFieldMeta = new HashMap<>();
    customFieldMeta.put("logicalType", "DECFLOAT");

    FieldType significandType =
        new FieldType(true, Types.MinorType.VARBINARY.getType(), null, customFieldMeta);
    FieldType exponentType =
        new FieldType(true, Types.MinorType.SMALLINT.getType(), null, customFieldMeta);

    StructVector vector = StructVector.empty("testVector", allocator);
    List<Field> fieldList = new ArrayList<>();
    fieldList.add(new Field("significand", significandType, null));
    fieldList.add(new Field("exponent", exponentType, null));
    vector.initializeChildrenFromFields(fieldList);

    VarBinaryVector significand = vector.getChild("significand", VarBinaryVector.class);
    SmallIntVector exponent = vector.getChild("exponent", SmallIntVector.class);

    for (int i = 0; i < values.length; i++) {
      if (values[i] == null) {
        significand.setNull(i);
        exponent.setNull(i);
      } else {
        BigInteger unscaled = values[i].unscaledValue();
        significand.setSafe(i, unscaled.toByteArray());
        exponent.setSafe(i, (short) -values[i].scale());
        vector.setIndexDefined(i);
      }
    }
    vector.setValueCount(values.length);
    return vector;
  }
}
