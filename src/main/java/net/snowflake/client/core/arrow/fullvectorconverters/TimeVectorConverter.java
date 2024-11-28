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
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public abstract class TimeVectorConverter<T extends BaseFixedWidthVector>
    extends AbstractFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;

  public TimeVectorConverter(RootAllocator allocator, ValueVector vector) {
    this.allocator = allocator;
    this.vector = vector;
  }

  protected FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Time(getTimeUnit(), getWidth()), null);
  }

  protected abstract TimeUnit getTimeUnit();

  protected abstract int getWidth();

  protected abstract T initVector();

  protected abstract void convertValue(T dstVector, int idx, long value);

  protected abstract int targetScale();

  @Override
  protected FieldVector convertVector()
      throws SFException, SnowflakeSQLException, SFArrowException {
    try {
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
      return converted;
    } finally {
      vector.close();
    }
  }
}
