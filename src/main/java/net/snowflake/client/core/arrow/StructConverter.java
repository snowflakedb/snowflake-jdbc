package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.StructVector;

@SnowflakeJdbcInternalApi
public class StructConverter extends AbstractArrowVectorConverter {

  private final StructVector structVector;

  public StructConverter(StructVector vector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.OBJECT.name(), vector, columnIndex, context);
    structVector = vector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    return structVector.getObject(index);
  }

  @Override
  public String toString(int index) throws SFException {
    return structVector.getObject(index).toString();
  }

  // TODO SNOW-1374896 fix toString
  // TODO SNOW-1374896 implement toBytes
}
