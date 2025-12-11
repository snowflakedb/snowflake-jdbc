package net.snowflake.client.core.arrow.fullvectorconverters;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.core.arrow.ArrowVectorConverterUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;

@SnowflakeJdbcInternalApi
public abstract class SimpleArrowFullVectorConverter<T extends FieldVector>
    extends AbstractFullVectorConverter {

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

  protected void additionalConverterInit(ArrowVectorConverter converter) {}

  protected FieldVector convertVector()
      throws SFException, SnowflakeSQLException, SFArrowException {
    if (matchingType()) {
      return (FieldVector) vector;
    }
    int size = vector.getValueCount();
    T converted = initVector();
    ArrowVectorConverter converter =
        ArrowVectorConverterUtil.initConverter(vector, context, session, idx);
    additionalConverterInit(converter);
    for (int i = 0; i < size; i++) {
      if (!vector.isNull(i)) {
        convertValue(converter, converted, i);
      }
    }
    converted.setValueCount(size);
    vector.close();
    return converted;
  }
}
