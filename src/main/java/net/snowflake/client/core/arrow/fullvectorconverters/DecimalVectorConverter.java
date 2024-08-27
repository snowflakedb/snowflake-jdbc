package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public class DecimalVectorConverter extends SimpleArrowFullVectorConverter<DecimalVector> {

  public DecimalVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  @Override
  protected boolean matchingType() {
    return (vector instanceof DecimalVector);
  }

  @Override
  protected DecimalVector initVector() {
    String scaleString = vector.getField().getMetadata().get("scale");
    String precisionString = vector.getField().getMetadata().get("precision");
    int scale = Integer.parseInt(scaleString);
    int precision = Integer.parseInt(precisionString);
    DecimalVector resultVector = new DecimalVector(vector.getName(), allocator, precision, scale);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, DecimalVector to, int idx)
      throws SFException {
    to.set(idx, from.toBigDecimal(idx));
  }
}
