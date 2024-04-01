package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.ListVector;

@SnowflakeJdbcInternalApi
public class ArrayConverter extends AbstractArrowVectorConverter {

  private final ListVector vector;

  public ArrayConverter(ListVector valueVector, int vectorIndex, DataConversionContext context) {
    super(SnowflakeType.ARRAY.name(), valueVector, vectorIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    return vector.getObject(index);
  }

  @Override
  public String toString(int index) throws SFException {
    return vector.getObject(index).toString();
  }
}
