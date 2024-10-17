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
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

@SnowflakeJdbcInternalApi
public class ListVectorConverter extends AbstractFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;
  protected DataConversionContext context;
  protected SFBaseSession session;
  protected int idx;
  protected Object valueTargetType;
  private TimeZone timeZoneToUse;

  ListVectorConverter(
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

  private static FieldType getFieldType(boolean nullable) {
    return new FieldType(nullable, new ArrowType.List(), null);
  }

  protected ListVector initVector(String name, Field field) {
    boolean nullable = vector.getField().isNullable();
    ListVector convertedListVector = new ListVector(name, allocator, getFieldType(nullable), null);
    ArrayList<Field> fields = new ArrayList<>();
    fields.add(field);
    convertedListVector.initializeChildrenFromFields(fields);
    return convertedListVector;
  }

  @Override
  protected FieldVector convertVector()
      throws SFException, SnowflakeSQLException, SFArrowException {
    try {
      ListVector listVector = (ListVector) vector;
      FieldVector dataVector = listVector.getDataVector();
      FieldVector convertedDataVector =
          ArrowFullVectorConverterUtil.convert(
              allocator, dataVector, context, session, timeZoneToUse, 0, valueTargetType);
      ListVector convertedListVector = initVector(vector.getName(), convertedDataVector.getField());
      convertedListVector.allocateNew();
      convertedListVector.setValueCount(listVector.getValueCount());
      convertedListVector.getOffsetBuffer().setBytes(0, listVector.getOffsetBuffer());
      ArrowBuf validityBuffer = listVector.getValidityBuffer();
      convertedListVector
          .getValidityBuffer()
          .setBytes(0L, validityBuffer, 0L, validityBuffer.capacity());
      convertedListVector.setLastSet(listVector.getLastSet());
      convertedDataVector.makeTransferPair(convertedListVector.getDataVector()).transfer();
      return convertedListVector;
    } finally {
      vector.close();
    }
  }
}
