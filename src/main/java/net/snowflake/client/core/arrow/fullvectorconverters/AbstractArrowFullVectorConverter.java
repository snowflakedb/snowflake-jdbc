package net.snowflake.client.core.arrow.fullvectorconverters;

import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeType;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;

public abstract class AbstractArrowFullVectorConverter
    implements ArrowFullVectorConverter {
  protected RootAllocator allocator;
  protected ValueVector vector;
  protected DataConversionContext context;
  protected SFBaseSession session;
  protected int idx;

  public AbstractArrowFullVectorConverter(
      RootAllocator allocator,
      ValueVector vector,
      DataConversionContext context,
      SFBaseSession session,
      int idx) {
    this.allocator = allocator;
    this.vector = vector;
    this.context = context;
    this.session = session;
    this.idx = idx;
  }

  private static Types.MinorType deduceType(ValueVector vector) {
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
          return Types.MinorType.TIMEMILLI;
        case TIMESTAMP_LTZ:
          {
            String scaleStr = vector.getField().getMetadata().get("scale");
            int sfScale = Integer.parseInt(scaleStr);
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
            String scaleStr = vector.getField().getMetadata().get("scale");
            int sfScale = Integer.parseInt(scaleStr);
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
            String scaleStr = vector.getField().getMetadata().get("scale");
            int sfScale = Integer.parseInt(scaleStr);
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
        }
      }
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex.getCause(), ex.getSqlState(), ex.getVendorCode(), ex.getParams());
    }
    return null;
  }
}
