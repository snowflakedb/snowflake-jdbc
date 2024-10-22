package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;

@SnowflakeJdbcInternalApi
public class VarCharVectorConverter extends SimpleArrowFullVectorConverter<VarCharVector> {
  public VarCharVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    super(allocator, vector, context, session, idx);
  }

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Utf8(), null);
  }

  @Override
  protected boolean matchingType() {
    return (vector instanceof VarCharVector);
  }

  @Override
  protected VarCharVector initVector() {
    boolean nullable = vector.getField().isNullable();
    VarCharVector resultVector =
        new VarCharVector(vector.getName(), getFieldType(nullable), allocator);
    resultVector.allocateNew(vector.getValueCount());
    return resultVector;
  }

  @Override
  protected void convertValue(ArrowVectorConverter from, VarCharVector to, int idx)
      throws SFException {
    to.set(idx, new Text(from.toString(idx)));
  }
}
