package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.tostringhelpers.ArrowArrayStringRepresentationBuilder;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;

public class ArrayConverter extends AbstractArrowVectorConverter {

  private final ListVector vector;

  public ArrayConverter(ListVector valueVector, int vectorIndex, DataConversionContext context) {
    super(SnowflakeType.ARRAY.name(), valueVector, vectorIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    return isNull(index)? null : toString(index);
  }

  @Override
  public String toString(int index) throws SFException {
    FieldVector vectorUnpacked = vector.getChildrenFromFields().get(0);
    SnowflakeType logicalType =
        ArrowVectorConverterUtil.getSnowflakeTypeFromFieldMetadata(vectorUnpacked.getField());

    ArrowArrayStringRepresentationBuilder builder =
        new ArrowArrayStringRepresentationBuilder(logicalType);

    final ArrowVectorConverter converter;

    try {
      converter = ArrowVectorConverterUtil.initConverter(vectorUnpacked, context, columnIndex);
    } catch (SnowflakeSQLException e) {
      return vector.getObject(index).toString();
    }

    for (int i = vector.getElementStartIndex(index); i < vector.getElementEndIndex(index); i++) {
      builder.appendValue(converter.toString(i));
    }

    return builder.toString();
  }
}
