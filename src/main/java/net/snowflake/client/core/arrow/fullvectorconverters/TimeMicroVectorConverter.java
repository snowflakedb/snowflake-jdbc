package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;

@SnowflakeJdbcInternalApi
public class TimeMicroVectorConverter extends TimeVectorConverter<TimeMicroVector> {

  public TimeMicroVectorConverter(RootAllocator allocator, ValueVector vector) {
    super(allocator, vector);
  }

  @Override
  protected TimeUnit getTimeUnit() {
    return TimeUnit.MICROSECOND;
  }

  @Override
  protected int getWidth() {
    return 64;
  }

  @Override
  protected TimeMicroVector initVector() {
    return new TimeMicroVector(
        vector.getName(), getFieldType(vector.getField().isNullable()), allocator);
  }

  @Override
  protected void convertValue(TimeMicroVector dstVector, int idx, long value) {
    dstVector.set(idx, value);
  }

  @Override
  protected int targetScale() {
    return 6;
  }
}
