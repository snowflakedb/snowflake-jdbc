package net.snowflake.client.core.arrow.fullvectorconverters;

import static net.snowflake.client.core.arrow.ArrowVectorConverterUtil.getScale;

import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public class ArrowFullVectorConverterUtil {
  private ArrowFullVectorConverterUtil() {}

  public static Types.MinorType deduceType(ValueVector vector, SFBaseSession session)
      throws SnowflakeSQLLoggedException {
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
            int sfScale = getScale(vector, session);
            if (sfScale != 0) {
              return Types.MinorType.DECIMAL;
            }
            break;
          }
        case TIME:
          return Types.MinorType.TIMEMILLI;
        case TIMESTAMP_LTZ:
          {
            int sfScale = getScale(vector, session);
            switch (sfScale) {
              case 0:
                return Types.MinorType.TIMESTAMPSECTZ;
              case 3:
                return Types.MinorType.TIMESTAMPMILLITZ;
              case 6:
                return Types.MinorType.TIMESTAMPMICROTZ;
              case 9:
                return Types.MinorType.TIMESTAMPNANOTZ;
            }
            break;
          }
        case TIMESTAMP_TZ:
          {
            int sfScale = getScale(vector, session);
            switch (sfScale) {
              case 0:
                return Types.MinorType.TIMESTAMPSECTZ;
              case 3:
                return Types.MinorType.TIMESTAMPMILLITZ;
              case 6:
                return Types.MinorType.TIMESTAMPMICROTZ;
              case 9:
                return Types.MinorType.TIMESTAMPNANOTZ;
            }
            break;
          }
        case TIMESTAMP_NTZ:
          {
            int sfScale = getScale(vector, session);
            switch (sfScale) {
              case 0:
                return Types.MinorType.TIMESTAMPSEC;
              case 3:
                return Types.MinorType.TIMESTAMPMILLI;
              case 6:
                return Types.MinorType.TIMESTAMPMICRO;
              case 9:
                return Types.MinorType.TIMESTAMPNANO;
            }
            break;
          }
        default:
          throw new SnowflakeSQLLoggedException(
              session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
              "Unexpected Arrow Field for ",
              st.name());
      }
    }
    return type;
  }

  public static FieldVector convert(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx,
      Object targetType)
      throws SnowflakeSQLException {
    try {
      if (targetType == null) {
        targetType = deduceType(vector, session);
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
          default:
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unsupported target type");
        }
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
    return null;
  }
}
