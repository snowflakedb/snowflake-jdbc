package net.snowflake.client.core.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.fullvectorconverters.ArrowFullVectorConverterUtil;
import net.snowflake.client.core.arrow.fullvectorconverters.IntVectorConverter;
import net.snowflake.client.core.arrow.fullvectorconverters.SFArrowException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.BaseRepeatedValueVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.JsonStringHashMap;
import org.apache.arrow.vector.util.Text;
import org.junit.Test;

public class ArrowBatchesTest extends BaseConverterTest {
  @Test
  public void testRepeatedConvert() throws SFException, SnowflakeSQLException, SFArrowException {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    IntVector intVector = new IntVector("test", allocator);
    intVector.allocateNew(2);
    intVector.set(0, 1);
    intVector.set(1, 4);
    intVector.setValueCount(2);

    IntVectorConverter converter = new IntVectorConverter(allocator, intVector, this, null, 0);
    IntVector convertedIntVector = (IntVector) converter.convert();
    assertEquals(convertedIntVector.getValueCount(), 2);
    assertEquals(convertedIntVector.get(0), 1);
    assertEquals(convertedIntVector.get(1), 4);
    try {
      converter.convert().clear();
    } catch (SFArrowException e) {
      // should throw
      return;
    }
    fail("Second conversion should throw");
  }

  @Test
  public void testUnknownType() {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    // Vector of unsupported type
    IntervalDayVector vector = new IntervalDayVector("test", allocator);
    try {
      ArrowFullVectorConverterUtil.convert(allocator, vector, this, null, null, 0, null);
    } catch (SFArrowException e) {
      assertTrue(e.getCause() instanceof SFArrowException);
      // should throw
      return;
    }
    fail("Should throw on unsupported type");
  }

  @Test
  public void testMapVectorConverter() throws SFArrowException {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    Map<String, String> metadata =
        new HashMap<String, String>() {
          {
            put("logicalType", "FIXED");
            put("scale", "3");
            put("precision", "18");
          }
        };
    FieldType valueVectorFieldType =
        new FieldType(false, new ArrowType.Int(32, true), null, metadata);
    IntVector decimalVector = new IntVector("value", valueVectorFieldType, allocator);
    decimalVector.allocateNew(2);
    decimalVector.set(0, 1);
    decimalVector.set(1, 4);
    decimalVector.setValueCount(2);

    FieldType keyVectorFieldType = new FieldType(false, new ArrowType.Utf8(), null);
    VarCharVector keyVector = new VarCharVector("key", keyVectorFieldType, allocator);
    keyVector.allocateNew(2);
    keyVector.set(0, "a".getBytes());
    keyVector.set(1, "b".getBytes());
    keyVector.setValueCount(2);

    FieldType entriesVectorFieldType = new FieldType(false, new ArrowType.Struct(), null);
    StructVector entryVector = new StructVector("entries", allocator, entriesVectorFieldType, null);
    entryVector.initializeChildrenFromFields(
        new ArrayList<Field>() {
          {
            add(decimalVector.getField());
            add(keyVector.getField());
          }
        });
    entryVector.allocateNew();
    entryVector.setValueCount(2);
    entryVector.getValidityBuffer().setByte(0, 3);
    decimalVector.makeTransferPair(entryVector.getChild("value")).transfer();
    keyVector.makeTransferPair(entryVector.getChild("key")).transfer();

    FieldType mapVectorFieldType = new FieldType(true, new ArrowType.Map(false), null);
    MapVector mapVector = new MapVector("map", allocator, mapVectorFieldType, null);
    mapVector.initializeChildrenFromFields(
        new ArrayList<Field>() {
          {
            add(entryVector.getField());
          }
        });
    mapVector.allocateNew();
    mapVector.setValueCount(1);
    mapVector.getOffsetBuffer().setByte(0, 0);
    mapVector.getOffsetBuffer().setByte(BaseRepeatedValueVector.OFFSET_WIDTH, 2);
    mapVector.getValidityBuffer().setByte(0, 1);
    entryVector.makeTransferPair(mapVector.getDataVector()).transfer();

    FieldVector convertedVector =
        ArrowFullVectorConverterUtil.convert(allocator, mapVector, this, null, null, 0, null);
    assertEquals(convertedVector.getField().getChildren().size(), 1);
    assertEquals(convertedVector.getField().getChildren().get(0).getType(), new ArrowType.Struct());
    assertEquals(convertedVector.getField().getChildren().get(0).getChildren().size(), 2);
    assertEquals(
        convertedVector.getField().getChildren().get(0).getChildren().get(1).getType(),
        new ArrowType.Utf8());
    assertEquals(
        convertedVector.getField().getChildren().get(0).getChildren().get(0).getType(),
        new ArrowType.Decimal(18, 3, 128));
    JsonStringArrayList<JsonStringHashMap<String, Object>> result =
        (JsonStringArrayList<JsonStringHashMap<String, Object>>) convertedVector.getObject(0);
    assertEquals(result.get(0).get("key"), new Text("a"));
    assertEquals(result.get(1).get("key"), new Text("b"));
    assertEquals(result.get(0).get("value"), BigDecimal.valueOf(0.001));
    assertEquals(result.get(1).get("value"), BigDecimal.valueOf(0.004));
  }
}
