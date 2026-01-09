package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.TransferPair;

@SnowflakeJdbcInternalApi
public class StructVectorConverter implements ArrowFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;
  protected DataConversionContext context;
  protected SFBaseSession session;
  private TimeZone timeZoneToUse;
  protected int idx;
  protected Map<String, Object> targetTypes;

  StructVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      TimeZone timeZoneToUse,
      int idx,
      Map<String, Object> targetTypes) {
    this.allocator = allocator;
    this.vector = vector;
    this.context = context;
    this.session = session;
    this.idx = idx;
    this.timeZoneToUse = timeZoneToUse;
    this.targetTypes = targetTypes;
  }

  public FieldVector convert() throws SFException, SnowflakeSQLException {
    StructVector structVector = (StructVector) vector;
    List<FieldVector> childVectors = structVector.getChildrenFromFields();
    List<FieldVector> convertedVectors = new ArrayList<>();
    for (FieldVector childVector : childVectors) {
      Object targetType = null;
      if (targetTypes != null) {
        targetType = targetTypes.get(childVector.getName());
      }
      convertedVectors.add(
          ArrowFullVectorConverter.convert(
              allocator, childVector, context, session, timeZoneToUse, idx, targetType));
    }

    List<Field> convertedFields =
        convertedVectors.stream().map(ValueVector::getField).collect(Collectors.toList());
    StructVector converted = StructVector.empty(vector.getName(), allocator);
    converted.allocateNew();
    converted.initializeChildrenFromFields(convertedFields);
    for (FieldVector convertedVector : convertedVectors) {
      TransferPair transferPair =
          convertedVector.makeTransferPair(converted.getChild(convertedVector.getName()));
      transferPair.transfer();
    }
    ArrowBuf validityBuffer = structVector.getValidityBuffer();
    converted.getValidityBuffer().setBytes(0L, validityBuffer, 0L, validityBuffer.capacity());
    converted.setValueCount(vector.getValueCount());

    vector.close();
    return converted;
  }
}
