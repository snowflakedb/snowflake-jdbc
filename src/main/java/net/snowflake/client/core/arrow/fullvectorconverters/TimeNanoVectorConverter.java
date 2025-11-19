package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public class TimeNanoVectorConverter extends TimeVectorConverter<TimeNanoVector> {

  public TimeNanoVectorConverter(RootAllocator allocator, ValueVector vector) {
    super(allocator, vector);
  }

  @Override
  protected TimeNanoVector initVector() {
    return new TimeNanoVector(vector.getName(), allocator);
  }

  @Override
  protected void convertValue(TimeNanoVector dstVector, int idx, long value) {
    dstVector.set(idx, value);
  }

  @Override
  protected int targetScale() {
    return 9;
  }
}
