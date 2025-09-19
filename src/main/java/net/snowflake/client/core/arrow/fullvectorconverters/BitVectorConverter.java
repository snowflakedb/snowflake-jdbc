package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public class BitVectorConverter extends SimpleArrowFullVectorConverter<BitVector> {

  public BitVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  @Override
  protected boolean matchingType() {
    return vector instanceof BitVector;
  }

  @Override
  protected BitVector initVector() {
    BitVector resultVector = new BitVector(vector.getName(), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, BitVector to, int idx) throws SFException {
    to.set(idx, from.toBoolean(idx) ? 1 : 0);
  }
}
