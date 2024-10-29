package net.snowflake.client.core.arrow;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.tostringhelpers.ArrowObjectStringRepresentationBuilder;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.MapVector;

public class MapConverter extends AbstractArrowVectorConverter {

  private final MapVector vector;

  public MapConverter(MapVector valueVector, int columnIndex, DataConversionContext context) {
    super(SnowflakeType.MAP.name(), valueVector, columnIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    return toString(index);
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return toString(index).getBytes();
  }

  @Override
  public String toString(int index) throws SFException {
    ArrowObjectStringRepresentationBuilder builder = new ArrowObjectStringRepresentationBuilder();

    FieldVector vectorUnpacked = vector.getChildrenFromFields().get(0);

    FieldVector keys = vectorUnpacked.getChildrenFromFields().get(0);
    FieldVector values = vectorUnpacked.getChildrenFromFields().get(1);
    final ArrowVectorConverter keyConverter;
    final ArrowVectorConverter valueConverter;

    SnowflakeType valueLogicalType =
        ArrowVectorConverterUtil.getSnowflakeTypeFromFieldMetadata(values.getField());

    try {
      keyConverter = ArrowVectorConverterUtil.initConverter(keys, context, columnIndex);
      valueConverter = ArrowVectorConverterUtil.initConverter(values, context, columnIndex);
    } catch (SnowflakeSQLException e) {
      return vector.getObject(index).toString();
    }

    for (int i = vector.getElementStartIndex(index); i < vector.getElementEndIndex(index); i++) {
      builder.appendKeyValue(
          keyConverter.toString(i), valueConverter.toString(i), valueLogicalType);
    }

    return builder.toString();
  }
}
