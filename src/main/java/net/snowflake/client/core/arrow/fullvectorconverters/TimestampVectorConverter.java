package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.core.arrow.ArrowResultUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.util.SFPair;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Field;

@SnowflakeJdbcInternalApi
public class TimestampVectorConverter implements ArrowFullVectorConverter {
  private RootAllocator allocator;
  private ValueVector vector;
  private DataConversionContext context;
  private TimeZone timeZoneToUse;
  private boolean isNTZ;

  /** Field names of the struct vectors used by timestamp */
  private static final String FIELD_NAME_EPOCH = "epoch"; // seconds since epoch

  private static final String FIELD_NAME_TIME_ZONE_INDEX = "timezone"; // time zone index
  private static final String FIELD_NAME_FRACTION = "fraction"; // fraction in nanoseconds

  public TimestampVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      TimeZone timeZoneToUse,
      boolean isNTZ) {
    this.allocator = allocator;
    this.vector = vector;
    this.context = context;
    this.timeZoneToUse = timeZoneToUse;
    this.isNTZ = isNTZ;
  }

  private IntVector makeVectorOfZeroes(int length) {
    IntVector vector = new IntVector(FIELD_NAME_FRACTION, allocator);
    vector.allocateNew(length);
    vector.zeroVector();
    vector.setValueCount(length);
    return vector;
  }

  private IntVector makeVectorOfUTCOffsets(int length) {
    IntVector vector = new IntVector(FIELD_NAME_TIME_ZONE_INDEX, allocator);
    vector.allocateNew(length);
    vector.setValueCount(length);
    for (int i = 0; i < length; i++) {
      vector.set(i, 1440);
    }
    return vector;
  }

  private SFPair<BigIntVector, IntVector> normalizeTimeSinceEpoch(BigIntVector vector) {
    int length = vector.getValueCount();
    int scale = Integer.parseInt(vector.getField().getMetadata().get("scale"));
    if (scale == 0) {
      IntVector fractions = makeVectorOfZeroes(length);
      fractions
          .getValidityBuffer()
          .setBytes(0L, vector.getValidityBuffer(), 0L, fractions.getValidityBuffer().capacity());
      return SFPair.of(vector, fractions);
    }
    long scaleFactor = ArrowResultUtil.powerOfTen(scale);
    long fractionScaleFactor = ArrowResultUtil.powerOfTen(9 - scale);
    BigIntVector epoch = new BigIntVector(FIELD_NAME_EPOCH, allocator);
    epoch.allocateNew(length);
    epoch.setValueCount(length);
    IntVector fractions = new IntVector(FIELD_NAME_FRACTION, allocator);
    fractions.allocateNew(length);
    fractions.setValueCount(length);
    for (int i = 0; i < length; i++) {
      epoch.set(i, vector.get(i) / scaleFactor);
      fractions.set(i, (int) ((vector.get(i) % scaleFactor) * fractionScaleFactor));
    }
    return SFPair.of(vector, fractions);
  }

  private IntVector makeTimeZoneOffsets(
      BigIntVector seconds, IntVector fractions, TimeZone timeZone) {
    IntVector offsets = new IntVector(FIELD_NAME_TIME_ZONE_INDEX, allocator);
    offsets.allocateNew(vector.getValueCount());
    for (int i = 0; i < vector.getValueCount(); i++) {
      offsets.set(
          i,
          1440
              + timeZone.getOffset(seconds.get(i) * 1000 + fractions.get(i) / 1000000)
                  / (1000 * 60));
    }
    return offsets;
  }

  private StructVector pack(BigIntVector seconds, IntVector fractions, IntVector offsets) {
    StructVector result = StructVector.empty(vector.getName(), allocator);
    List<Field> fields =
        new ArrayList<Field>() {
          {
            add(seconds.getField());
            add(fractions.getField());
            add(offsets.getField());
          }
        };
    result.setInitialCapacity(seconds.getValueCount());
    result.initializeChildrenFromFields(fields);
    seconds.makeTransferPair(result.getChild(FIELD_NAME_EPOCH)).transfer();
    fractions.makeTransferPair(result.getChild(FIELD_NAME_FRACTION)).transfer();
    offsets.makeTransferPair(result.getChild(FIELD_NAME_TIME_ZONE_INDEX)).transfer();
    result.setValueCount(vector.getValueCount());
    result
        .getValidityBuffer()
        .setBytes(0L, vector.getValidityBuffer(), 0L, vector.getValidityBuffer().capacity());
    return result;
  }

  @Override
  public FieldVector convert() throws SFException, SnowflakeSQLException {
    BigIntVector seconds;
    IntVector fractions;
    IntVector timeZoneIndices = null;
    if (vector instanceof BigIntVector) {
      SFPair<BigIntVector, IntVector> normalized = normalizeTimeSinceEpoch((BigIntVector) vector);
      seconds = normalized.left;
      fractions = normalized.right;
    } else {
      StructVector structVector = (StructVector) vector;
      if (structVector.getChildrenFromFields().size() == 3) {
        return structVector;
      }
      if (structVector.getChild(FIELD_NAME_FRACTION) == null) {
        SFPair<BigIntVector, IntVector> normalized =
            normalizeTimeSinceEpoch(structVector.getChild(FIELD_NAME_EPOCH, BigIntVector.class));
        seconds = normalized.left;
        fractions = normalized.right;
      } else {
        seconds = structVector.getChild(FIELD_NAME_EPOCH, BigIntVector.class);
        fractions = structVector.getChild(FIELD_NAME_FRACTION, IntVector.class);
      }
      timeZoneIndices = structVector.getChild(FIELD_NAME_TIME_ZONE_INDEX, IntVector.class);
    }
    if (timeZoneIndices == null) {
      if (isNTZ && context.getHonorClientTZForTimestampNTZ()) {
        timeZoneIndices = makeTimeZoneOffsets(seconds, fractions, TimeZone.getDefault());
        for (int i = 0; i < vector.getValueCount(); i++) {
          seconds.set(i, seconds.get(i) - (timeZoneIndices.get(i) - 1440) * 60L);
        }
      } else if (isNTZ || timeZoneToUse == null) {
        timeZoneIndices = makeVectorOfUTCOffsets(vector.getValueCount());
      } else {
        timeZoneIndices = makeTimeZoneOffsets(seconds, fractions, timeZoneToUse);
      }
    }
    return pack(seconds, fractions, timeZoneIndices);
  }
}

 /*         case TIMESTAMPNANOTZ:
         return new TimestampVectorConverter(allocator, vector, context, timeZoneToUse, false)
                 .convert();
           case TIMESTAMPNANO:
         return new TimestampVectorConverter(allocator, vector, context, timeZoneToUse, true)
                 .convert();

 private void testTimestampBase(String query) throws Exception, SFException {
     Statement statement = connection.createStatement();
     ResultSet rs = statement.executeQuery(query);
     ArrowBatches batches = rs.unwrap(SnowflakeResultSet.class).getArrowBatches();

     ArrowBatch batch = batches.next();
     VectorSchemaRoot root = batch.fetch().get(0);
     assertTrue(root.getVector(0) instanceof StructVector);
     ArrowVectorConverter converter = batch.getTimestampConverter(root.getVector(0), 1);
     Timestamp tsFromBatch = converter.toTimestamp(0, null);

     rs = statement.executeQuery(query);
     rs.next();
     Timestamp tsFromRow = rs.getTimestamp(1);

     assertTrue(tsFromBatch.equals(tsFromRow));
     root.close();
 }

 @Test
 public void testTimestampTZBatch() throws Exception, SFException {
     testTimestampBase("select '2020-04-05 12:22:12'::TIMESTAMP_TZ");
 }

 @Test
 public void testTimestampLTZUseSessionTimezoneBatch() throws Exception, SFException {
     Statement statement = connection.createStatement();
     statement.execute("alter session set JDBC_USE_SESSION_TIMEZONE=true");
     testTimestampBase("select '2020-04-05 12:22:12'::TIMESTAMP_LTZ");
     statement.execute("alter session unset JDBC_USE_SESSION_TIMEZONE");
 }

 @Test
 public void testTimestampLTZBatch() throws Exception, SFException {
     testTimestampBase("select '2020-04-05 12:22:12+0700'::TIMESTAMP_LTZ");
 }

 @Test
 public void testTimestampNTZBatch() throws Exception, SFException {
     testTimestampBase("select '2020-04-05 12:22:12'::TIMESTAMP_NTZ");
 }*/
