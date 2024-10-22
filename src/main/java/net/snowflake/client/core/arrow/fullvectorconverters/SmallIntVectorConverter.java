package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class SmallIntVectorConverter extends SimpleArrowFullVectorConverter<SmallIntVector> {

  public SmallIntVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Int(16, true), null);
  }

  @Override
  protected boolean matchingType() {
    return (vector instanceof SmallIntVector);
  }

  @Override
  protected SmallIntVector initVector() {
    boolean nullable = vector.getField().isNullable();
    SmallIntVector resultVector =
        new SmallIntVector(vector.getName(), getFieldType(nullable), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, SmallIntVector to, int idx)
      throws SFException {
    to.set(idx, from.toShort(idx));
  }
}
