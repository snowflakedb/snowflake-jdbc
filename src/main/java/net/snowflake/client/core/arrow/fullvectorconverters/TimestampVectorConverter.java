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

  // This parameter is used to distinguish between NTZ and LTZ (TZ is distinct in having the offset
  // vector)
  private boolean isNTZ;

  /** Field names of the struct vectors used by timestamp */
  private static final String FIELD_NAME_EPOCH = "epoch"; // seconds since epoch

  private static final String FIELD_NAME_TIME_ZONE_INDEX = "timezone"; // time zone index
  private static final String FIELD_NAME_FRACTION = "fraction"; // fraction in nanoseconds
  private static final int UTC_OFFSET = 1440;
  private static final long NANOS_PER_MILLI = 1000000L;
  private static final int MILLIS_PER_SECOND = 1000;
  private static final int SECONDS_PER_MINUTE = 60;

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
      vector.set(i, UTC_OFFSET);
    }
    return vector;
  }

  private SFPair<BigIntVector, IntVector> normalizeTimeSinceEpoch(BigIntVector vector) {
    int length = vector.getValueCount();
    int scale = Integer.parseInt(vector.getField().getMetadata().get("scale"));
    if (scale == 0) {
      IntVector fractions = makeVectorOfZeroes(length);
      BigIntVector epoch = new BigIntVector(FIELD_NAME_EPOCH, allocator);
      fractions
          .getValidityBuffer()
          .setBytes(0L, vector.getValidityBuffer(), 0L, vector.getValidityBuffer().capacity());
      vector.makeTransferPair(epoch).transfer();
      return SFPair.of(epoch, fractions);
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
    return SFPair.of(epoch, fractions);
  }

  private IntVector makeTimeZoneOffsets(
      BigIntVector seconds, IntVector fractions, TimeZone timeZone) {
    IntVector offsets = new IntVector(FIELD_NAME_TIME_ZONE_INDEX, allocator);
    offsets.allocateNew(seconds.getValueCount());
    offsets.setValueCount(seconds.getValueCount());
    for (int i = 0; i < seconds.getValueCount(); i++) {
      offsets.set(
          i,
          UTC_OFFSET
              + timeZone.getOffset(
                      seconds.get(i) * MILLIS_PER_SECOND + fractions.get(i) / NANOS_PER_MILLI)
                  / (MILLIS_PER_SECOND * SECONDS_PER_MINUTE));
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
    result.setValueCount(seconds.getValueCount());
    result
        .getValidityBuffer()
        .setBytes(0L, seconds.getValidityBuffer(), 0L, seconds.getValidityBuffer().capacity());
    return result;
  }

  @Override
  public FieldVector convert() throws SFException, SnowflakeSQLException {
    boolean returnedOriginal = false;
    try {
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
          returnedOriginal = true;
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
          for (int i = 0; i < seconds.getValueCount(); i++) {
            seconds.set(
                i,
                seconds.get(i) - (long) (timeZoneIndices.get(i) - UTC_OFFSET) * SECONDS_PER_MINUTE);
          }
        } else if (isNTZ || timeZoneToUse == null) {
          timeZoneIndices = makeVectorOfUTCOffsets(seconds.getValueCount());
        } else {
          timeZoneIndices = makeTimeZoneOffsets(seconds, fractions, timeZoneToUse);
        }
      }
      return pack(seconds, fractions, timeZoneIndices);
    } finally {
      if (!returnedOriginal) {
        vector.close();
      }
    }
  }
}
