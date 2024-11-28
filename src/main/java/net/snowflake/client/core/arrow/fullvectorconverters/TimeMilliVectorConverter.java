package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;

@SnowflakeJdbcInternalApi
public class TimeMilliVectorConverter extends TimeVectorConverter<TimeMilliVector> {
  public TimeMilliVectorConverter(RootAllocator allocator, ValueVector vector) {
    super(allocator, vector);
  }

  @Override
  protected TimeUnit getTimeUnit() {
    return TimeUnit.MILLISECOND;
  }

  @Override
  protected int getWidth() {
    return 32;
  }

  @Override
  protected TimeMilliVector initVector() {
    return new TimeMilliVector(
        vector.getName(), getFieldType(vector.getField().isNullable()), allocator);
  }

  @Override
  protected void convertValue(TimeMilliVector dstVector, int idx, long value) {
    dstVector.set(idx, (int) value);
  }

  @Override
  protected int targetScale() {
    return 3;
  }
}
