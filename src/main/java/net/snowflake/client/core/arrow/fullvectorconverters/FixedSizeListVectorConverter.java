package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.ArrayList;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class FixedSizeListVectorConverter extends AbstractFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;
  protected DataConversionContext context;
  protected SFBaseSession session;
  protected int idx;
  protected Object valueTargetType;
  private TimeZone timeZoneToUse;

  FixedSizeListVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      TimeZone timeZoneToUse,
      int idx,
      Object valueTargetType) {
    this.allocator = allocator;
    this.vector = vector;
    this.context = context;
    this.session = session;
    this.timeZoneToUse = timeZoneToUse;
    this.idx = idx;
    this.valueTargetType = valueTargetType;
  }

  private static FieldType getFieldType(boolean nullable, int size) {
    return new FieldType(nullable, new ArrowType.FixedSizeList(size), null);
  }

  @Override
  protected FieldVector convertVector()
      throws SFException, SnowflakeSQLException, SFArrowException {
    try {
      FixedSizeListVector listVector = (FixedSizeListVector) vector;
      FieldVector dataVector = listVector.getDataVector();
      FieldVector convertedDataVector =
          ArrowFullVectorConverterUtil.convert(
              allocator, dataVector, context, session, timeZoneToUse, 0, valueTargetType);

      boolean nullable = vector.getField().isNullable();
      int listSize = listVector.getListSize();
      FixedSizeListVector convertedListVector =
          new FixedSizeListVector(
              listVector.getName(), allocator, getFieldType(nullable, listSize), null);
      ArrayList<Field> fields = new ArrayList<>();
      fields.add(convertedDataVector.getField());
      convertedListVector.initializeChildrenFromFields(fields);
      convertedListVector.allocateNew();
      convertedListVector.setValueCount(listVector.getValueCount());
      ArrowBuf validityBuffer = listVector.getValidityBuffer();
      convertedListVector
          .getValidityBuffer()
          .setBytes(0L, validityBuffer, 0L, validityBuffer.capacity());
      convertedDataVector.makeTransferPair(convertedListVector.getDataVector()).transfer();
      return convertedListVector;
    } finally {
      vector.close();
    }
  }
}
