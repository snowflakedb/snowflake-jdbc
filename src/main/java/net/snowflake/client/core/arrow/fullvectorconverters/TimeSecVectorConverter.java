package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.TimeUnit;

@SnowflakeJdbcInternalApi
public class TimeSecVectorConverter extends TimeVectorConverter<TimeSecVector> {
  public TimeSecVectorConverter(RootAllocator allocator, ValueVector vector) {
    super(allocator, vector);
  }

  @Override
  protected TimeUnit getTimeUnit() {
    return TimeUnit.SECOND;
  }

  @Override
  protected int getWidth() {
    return 32;
  }

  @Override
  protected TimeSecVector initVector() {
    return new TimeSecVector(
        vector.getName(), getFieldType(vector.getField().isNullable()), allocator);
  }

  @Override
  protected void convertValue(TimeSecVector dstVector, int idx, long value) {
    dstVector.set(idx, (int) value);
  }

  @Override
  protected int targetScale() {
    return 0;
  }
}
