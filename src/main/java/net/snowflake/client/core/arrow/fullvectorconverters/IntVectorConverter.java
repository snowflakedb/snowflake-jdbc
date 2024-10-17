package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class IntVectorConverter extends SimpleArrowFullVectorConverter<IntVector> {

  public IntVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  @Override
  protected boolean matchingType() {
    return (vector instanceof IntVector);
  }

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Int(32, true), null);
  }

  @Override
  protected IntVector initVector() {
    boolean nullable = vector.getField().isNullable();
    IntVector resultVector = new IntVector(vector.getName(), getFieldType(nullable), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, IntVector to, int idx) throws SFException {
    to.set(idx, from.toInt(idx));
  }
}
