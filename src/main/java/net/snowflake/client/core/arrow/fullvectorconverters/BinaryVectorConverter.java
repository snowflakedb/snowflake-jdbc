package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;

@SnowflakeJdbcInternalApi
public class BinaryVectorConverter extends SimpleArrowFullVectorConverter<VarBinaryVector> {
  public BinaryVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  @Override
  protected boolean matchingType() {
    return vector instanceof VarBinaryVector;
  }

  @Override
  protected VarBinaryVector initVector() {
    VarBinaryVector resultVector = new VarBinaryVector(vector.getName(), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, VarBinaryVector to, int idx)
      throws SFException {
    to.set(idx, from.toBytes(idx));
  }
}
