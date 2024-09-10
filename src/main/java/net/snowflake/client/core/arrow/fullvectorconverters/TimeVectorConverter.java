package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowResultUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public abstract class TimeVectorConverter<T extends BaseFixedWidthVector>
    implements ArrowFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;

  public TimeVectorConverter(RootAllocator allocator, ValueVector vector) {
    this.allocator = allocator;
    this.vector = vector;
  }

  protected abstract T initVector();

  protected abstract void convertValue(T dstVector, int idx, long value);

  protected abstract int targetScale();

  @Override
  public FieldVector convert() throws SFException, SnowflakeSQLException {
    int size = vector.getValueCount();
    T converted = initVector();
    converted.allocateNew(size);
    BaseIntVector srcVector = (BaseIntVector) vector;
    int scale = Integer.parseInt(vector.getField().getMetadata().get("scale"));
    long scalingFactor = ArrowResultUtil.powerOfTen(targetScale() - scale);
    for (int i = 0; i < size; i++) {
      convertValue(converted, i, srcVector.getValueAsLong(i) * scalingFactor);
    }
    converted.setValueCount(size);
    vector.close();
    return converted;
  }
}
