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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DecfloatToDecimalConverterTest extends BaseConverterTest {

  private BufferAllocator allocator;
  private StructVector structVector;

  @BeforeEach
  public void setupAllocator() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void closeVector() {
    if (structVector != null) {
      structVector.close();
      structVector = null;
    }
    if (allocator != null) {
      allocator.close();
    }
  }

  @Test
  public void testFormatDecfloatMatchesTicketExamples() {
    assertEquals("0", DecfloatToDecimalConverter.formatDecfloat(BigInteger.ZERO, 0));
    assertEquals("123.456", formatFromLiteral("123.456"));
    assertEquals("1.2e200", formatFromLiteral("1.2e200"));
    assertEquals("1e16384", formatFromLiteral("1E+16384"));
    assertEquals(
        "1.2345678901234567890123456789012345678e100",
        formatFromLiteral("1.2345678901234567890123456789012345678E+100"));
    assertEquals("-1.234e8000", formatFromLiteral("-1.234E+8000"));
    assertEquals("0.000000123", formatFromLiteral("1.23E-7"));
  }

  @Test
  public void testFormatDecfloatWholeNumbersStayPlain() {
    // significand=1, exponent=2 → scale -2. BigDecimal.toString() would emit "1E+2".
    assertEquals("100", DecfloatToDecimalConverter.formatDecfloat(BigInteger.ONE, 2));
    assertEquals("1000000", DecfloatToDecimalConverter.formatDecfloat(BigInteger.ONE, 6));
    assertEquals("100", formatFromLiteral("100"));
    assertEquals("1000000", formatFromLiteral("1000000"));
  }

  @Test
  public void testToStringUsesOdbcStyleFormatting() throws SFException {
    structVector =
        createDecfloatVector(
            new BigDecimal("1.2e200"),
            new BigDecimal("123.456"),
            new BigDecimal(BigInteger.ONE, -2),
            new BigDecimal(BigInteger.ONE, -6),
            new BigDecimal("0"),
            null);
    ArrowVectorConverter converter = new DecfloatToDecimalConverter(structVector, 0, this);

    assertEquals("1.2e200", converter.toString(0));
    assertEquals("123.456", converter.toString(1));
    assertEquals("100", converter.toString(2));
    assertEquals("1000000", converter.toString(3));
    assertEquals("0", converter.toString(4));
    assertNull(converter.toString(5));
    assertEquals(new BigDecimal("1.2e200"), converter.toBigDecimal(0));
    assertEquals(new BigDecimal("123.456"), converter.toBigDecimal(1));
    assertEquals(0, new BigDecimal("100").compareTo(converter.toBigDecimal(2)));
    assertEquals(0, new BigDecimal("1000000").compareTo(converter.toBigDecimal(3)));
  }

  private static String formatFromLiteral(String literal) {
    BigDecimal value = new BigDecimal(literal);
    return DecfloatToDecimalConverter.formatDecfloat(value.unscaledValue(), -value.scale());
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
