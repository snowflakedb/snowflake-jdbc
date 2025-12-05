package net.snowflake.client.core.arrow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.fullvectorconverters.ArrowFullVectorConverterUtil;
import net.snowflake.client.core.arrow.fullvectorconverters.IntVectorConverter;
import net.snowflake.client.core.arrow.fullvectorconverters.SFArrowException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
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
}
