package net.snowflake.client.core.arrow;

import java.nio.charset.StandardCharsets;
import java.util.List;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.vector.complex.FixedSizeListVector;

/** Arrow FixedSizeListVector converter. */
public class VectorTypeConverter extends AbstractArrowVectorConverter {

  private final FixedSizeListVector vector;

  /**
   * @param valueVector ValueVector
   * @param vectorIndex vector index
   * @param context DataConversionContext
   */
  public VectorTypeConverter(
      FixedSizeListVector valueVector, int vectorIndex, DataConversionContext context) {
    super(SnowflakeType.ARRAY.name(), valueVector, vectorIndex, context);
    this.vector = valueVector;
  }

  @Override
  public Object toObject(int index) throws SFException {
    if (isNull(index)) {
      return null;
    }
    Object object = vector.getObject(index);
    return new StructObjectWrapper(object.toString(), object);
  }

  @Override
  public byte[] toBytes(int index) throws SFException {
    return isNull(index) ? null : toString(index).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public String toString(int index) throws SFException {
    List<?> object = vector.getObject(index);
    if (object == null) {
      return null;
    }
    return object.toString();
  }
}
