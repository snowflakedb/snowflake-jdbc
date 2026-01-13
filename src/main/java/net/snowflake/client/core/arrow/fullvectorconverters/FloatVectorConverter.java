package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public class FloatVectorConverter extends SimpleArrowFullVectorConverter<Float8Vector> {

  public FloatVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  @Override
  protected boolean matchingType() {
    return vector instanceof Float8Vector;
  }

  @Override
  protected Float8Vector initVector() {
    Float8Vector resultVector = new Float8Vector(vector.getName(), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, Float8Vector to, int idx)
      throws SFException {
    to.set(idx, from.toDouble(idx));
  }
}
