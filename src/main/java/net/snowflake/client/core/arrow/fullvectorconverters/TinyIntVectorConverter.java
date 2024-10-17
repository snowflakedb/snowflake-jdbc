package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class TinyIntVectorConverter extends SimpleArrowFullVectorConverter<TinyIntVector> {

  public TinyIntVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Int(8, true), null);
  }

  @Override
  protected boolean matchingType() {
    return (vector instanceof TinyIntVector);
  }

  @Override
  protected TinyIntVector initVector() {
    boolean nullable = vector.getField().isNullable();
    TinyIntVector resultVector =
        new TinyIntVector(vector.getName(), getFieldType(nullable), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, TinyIntVector to, int idx)
      throws SFException {
    to.set(idx, from.toByte(idx));
  }
}
