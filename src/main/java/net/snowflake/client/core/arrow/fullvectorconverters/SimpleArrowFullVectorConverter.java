package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public abstract class SimpleArrowFullVectorConverter<T extends FieldVector>
    implements ArrowFullVectorConverter {

  protected RootAllocator allocator;
  protected ValueVector vector;
  protected DataConversionContext context;
  protected SFBaseSession session;
  protected int idx;

  public SimpleArrowFullVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    this.allocator = allocator;
    this.vector = vector;
    this.context = context;
    this.session = session;
    this.idx = idx;
  }

  protected abstract boolean matchingType();

  protected abstract T initVector();

  protected abstract void convertValue(ArrowVectorConverter from, T to, int idx) throws SFException;

  public FieldVector convert() throws SFException, SnowflakeSQLException {
    if (matchingType()) {
      return (FieldVector) vector;
    }
    int size = vector.getValueCount();
    T converted = initVector();
    ArrowVectorConverter converter =
        ArrowVectorConverter.initConverter(vector, context, session, idx);
    for (int i = 0; i < size; i++) {
      convertValue(converter, converted, i);
    }
    converted.setValueCount(size);
    return converted;
  }
}
