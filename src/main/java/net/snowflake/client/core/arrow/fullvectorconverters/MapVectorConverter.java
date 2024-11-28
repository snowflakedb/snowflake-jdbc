package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.ArrayList;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class MapVectorConverter extends ListVectorConverter {

  MapVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      TimeZone timeZoneToUse,
      int idx,
      Object valueTargetType) {
    super(allocator, vector, context, session, timeZoneToUse, idx, valueTargetType);
  }

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.Map(false), null);
  }

  @Override
  protected ListVector initVector(String name, Field field) {
    boolean nullable = vector.getField().isNullable();
    MapVector convertedMapVector =
        new MapVector(vector.getName(), allocator, getFieldType(nullable), null);
    ArrayList<Field> fields = new ArrayList<>();
    fields.add(field);
    convertedMapVector.initializeChildrenFromFields(fields);
    return convertedMapVector;
  }
}
