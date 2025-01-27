package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.tostringhelpers.ArrowObjectStringRepresentationBuilder;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.FieldVector;
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
    return isNull(index) ? null : structVector.getObject(index);
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return isNull(index) ? null : toString(index).getBytes();
  }

  @Override
  public String toString(int index) throws SFException {
    ArrowObjectStringRepresentationBuilder builder = new ArrowObjectStringRepresentationBuilder();
    for (String childName : structVector.getChildFieldNames()) {
      FieldVector fieldVector = structVector.getChild(childName);
      SnowflakeType logicalType =
          ArrowVectorConverterUtil.getSnowflakeTypeFromFieldMetadata(fieldVector.getField());
      try {
        if (fieldVector.isNull(index)) {
          builder.appendKeyValue(childName, null, logicalType);
        } else {
          ArrowVectorConverter converter =
              ArrowVectorConverterUtil.initConverter(fieldVector, context, columnIndex);
          builder.appendKeyValue(childName, converter.toString(index), logicalType);
        }
      } catch (SnowflakeSQLException e) {
        return structVector.getObject(index).toString();
      }
    }
    return builder.toString();
  }
}
