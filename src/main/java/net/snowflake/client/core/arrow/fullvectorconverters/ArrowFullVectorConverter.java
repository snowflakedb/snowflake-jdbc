package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.Map;
import java.util.TimeZone;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

@SnowflakeJdbcInternalApi
public interface ArrowFullVectorConverter {
  static Types.MinorType deduceType(ValueVector vector) {
    Types.MinorType type = Types.getMinorTypeForArrowType(vector.getField().getType());
    // each column's metadata
    Map<String, String> customMeta = vector.getField().getMetadata();
    if (type == Types.MinorType.DECIMAL) {
      // Note: Decimal vector is different from others
      return Types.MinorType.DECIMAL;
    } else if (!customMeta.isEmpty()) {
      SnowflakeType st = SnowflakeType.valueOf(customMeta.get("logicalType"));
      switch (st) {
        case FIXED:
          {
            String scaleStr = vector.getField().getMetadata().get("scale");
            int sfScale = Integer.parseInt(scaleStr);
            if (sfScale != 0) {
              return Types.MinorType.DECIMAL;
            }
            break;
          }
        case TIME:
          {
            String scaleStr = vector.getField().getMetadata().get("scale");
            int sfScale = Integer.parseInt(scaleStr);
            if (sfScale == 0) {
              return Types.MinorType.TIMESEC;
            }
            if (sfScale <= 3) {
              return Types.MinorType.TIMEMILLI;
            }
            if (sfScale <= 6) {
              return Types.MinorType.TIMEMICRO;
            }
            if (sfScale <= 9) {
              return Types.MinorType.TIMENANO;
            }
          }
        case TIMESTAMP_NTZ:
          return Types.MinorType.TIMESTAMPNANO;
        case TIMESTAMP_LTZ:
        case TIMESTAMP_TZ:
          return Types.MinorType.TIMESTAMPNANOTZ;
      }
    }
    return type;
  }

  static FieldVector convert(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      TimeZone timeZoneToUse,
      int idx,
      Object targetType)
      throws SnowflakeSQLException {
    try {
      if (targetType == null) {
        targetType = deduceType(vector);
      }
      if (targetType instanceof Types.MinorType) {
        switch ((Types.MinorType) targetType) {
          case TINYINT:
            return new TinyIntVectorConverter(allocator, vector, context, session, idx).convert();
          case SMALLINT:
            return new SmallIntVectorConverter(allocator, vector, context, session, idx).convert();
          case INT:
            return new IntVectorConverter(allocator, vector, context, session, idx).convert();
          case BIGINT:
            return new BigIntVectorConverter(allocator, vector, context, session, idx).convert();
          case DECIMAL:
            return new DecimalVectorConverter(allocator, vector, context, session, idx).convert();
          case FLOAT8:
            return new FloatVectorConverter(allocator, vector, context, session, idx).convert();
          case BIT:
            return new BitVectorConverter(allocator, vector, context, session, idx).convert();
          case VARBINARY:
            return new BinaryVectorConverter(allocator, vector, context, session, idx).convert();
          case DATEDAY:
            return new DateVectorConverter(allocator, vector, context, session, idx, timeZoneToUse)
                .convert();
          case TIMESEC:
            return new TimeSecVectorConverter(allocator, vector).convert();
          case TIMEMILLI:
            return new TimeMilliVectorConverter(allocator, vector).convert();
          case TIMEMICRO:
            return new TimeMicroVectorConverter(allocator, vector).convert();
          case TIMENANO:
            return new TimeNanoVectorConverter(allocator, vector).convert();
          case TIMESTAMPNANOTZ:
            return new TimestampVectorConverter(allocator, vector, context, timeZoneToUse, false)
                .convert();
          case TIMESTAMPNANO:
            return new TimestampVectorConverter(allocator, vector, context, timeZoneToUse, true)
                .convert();
        }
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
    return null;
  }

  FieldVector convert() throws SFException, SnowflakeSQLException;
}