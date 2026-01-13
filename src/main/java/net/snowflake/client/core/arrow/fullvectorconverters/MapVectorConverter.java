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
import org.apache.arrow.vector.types.pojo.Field;

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

  @Override
  protected ListVector initVector(String name, Field field) {
    MapVector convertedMapVector = MapVector.empty(name, allocator, false);
    ArrayList<Field> fields = new ArrayList<>();
    fields.add(field);
    convertedMapVector.initializeChildrenFromFields(fields);
    return convertedMapVector;
  }
}
