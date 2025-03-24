package net.snowflake.client.core.arrow;

import java.util.Map;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.jdbc.ErrorCode;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

@SnowflakeJdbcInternalApi
public final class ArrowVectorConverterUtil {
  private ArrowVectorConverterUtil() {}

  public static SnowflakeType getSnowflakeTypeFromFieldMetadata(Field field) {
    Map<String, String> customMeta = field.getMetadata();
    if (customMeta != null && customMeta.containsKey("logicalType")) {
      return SnowflakeType.valueOf(customMeta.get("logicalType"));
    }

    return null;
  }

  /**
   * Given an arrow vector (a single column in a single record batch), return an arrow vector
   * converter. Note, converter is built on top of arrow vector, so that arrow data can be converted
   * back to java data
   *
   * <p>Arrow converter mappings for Snowflake fixed-point numbers
   * ----------------------------------------------------------------------------------------- Max
   * position and scale Converter
   * -----------------------------------------------------------------------------------------
   * number(3,0) {@link TinyIntToFixedConverter} number(3,2) {@link TinyIntToScaledFixedConverter}
   * number(5,0) {@link SmallIntToFixedConverter} number(5,4) {@link SmallIntToScaledFixedConverter}
   * number(10,0) {@link IntToFixedConverter} number(10,9) {@link IntToScaledFixedConverter}
   * number(19,0) {@link BigIntToFixedConverter} number(19,18) {@link BigIntToFixedConverter}
   * number(38,37) {@link DecimalToScaledFixedConverter}
   * ------------------------------------------------------------------------------------------
   *
   * @param vector an arrow vector
   * @param context data conversion context
   * @param session SFBaseSession for purposes of logging
   * @param idx the index of the vector in its batch
   * @return A converter on top og the vector
   * @throws SnowflakeSQLException if error encountered
   */
  public static ArrowVectorConverter initConverter(
      ValueVector vector, DataConversionContext context, SFBaseSession session, int idx)
      throws SnowflakeSQLException {
    // arrow minor type
    Types.MinorType type = Types.getMinorTypeForArrowType(vector.getField().getType());

    // each column's metadata
    SnowflakeType st = getSnowflakeTypeFromFieldMetadata(vector.getField());
    if (type == Types.MinorType.DECIMAL) {
      // Note: Decimal vector is different from others
      return new DecimalToScaledFixedConverter(vector, idx, context);
    } else if (st != null) {
      switch (st) {
        case ANY:
        case CHAR:
        case TEXT:
        case VARIANT:
          return new VarCharConverter(vector, idx, context);

        case MAP:
          if (vector instanceof MapVector) {
            return new MapConverter((MapVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case VECTOR:
          return new VectorTypeConverter((FixedSizeListVector) vector, idx, context);

        case ARRAY:
          if (vector instanceof ListVector) {
            return new ArrayConverter((ListVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case OBJECT:
          if (vector instanceof StructVector) {
            return new StructConverter((StructVector) vector, idx, context);
          } else {
            return new VarCharConverter(vector, idx, context);
          }

        case BINARY:
          return new VarBinaryToBinaryConverter(vector, idx, context);

        case BOOLEAN:
          return new BitToBooleanConverter(vector, idx, context);

        case DATE:
          boolean getFormatDateWithTimeZone = false;
          if (context.getSession() != null) {
            getFormatDateWithTimeZone = context.getSession().getFormatDateWithTimezone();
          }
          return new DateConverter(vector, idx, context, getFormatDateWithTimeZone);

        case FIXED:
          String scaleStr = vector.getField().getMetadata().get("scale");
          int sfScale = Integer.parseInt(scaleStr);
          switch (type) {
            case TINYINT:
              if (sfScale == 0) {
                return new TinyIntToFixedConverter(vector, idx, context);
              } else {
                return new TinyIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case SMALLINT:
              if (sfScale == 0) {
                return new SmallIntToFixedConverter(vector, idx, context);
              } else {
                return new SmallIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case INT:
              if (sfScale == 0) {
                return new IntToFixedConverter(vector, idx, context);
              } else {
                return new IntToScaledFixedConverter(vector, idx, context, sfScale);
              }
            case BIGINT:
              if (sfScale == 0) {
                return new BigIntToFixedConverter(vector, idx, context);
              } else {
                return new BigIntToScaledFixedConverter(vector, idx, context, sfScale);
              }
          }
          break;

        case REAL:
          return new DoubleToRealConverter(vector, idx, context);

        case TIME:
          switch (type) {
            case INT:
              return new IntToTimeConverter(vector, idx, context);
            case BIGINT:
              return new BigIntToTimeConverter(vector, idx, context);
            default:
              throw new SnowflakeSQLLoggedException(
                  session,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  SqlState.INTERNAL_ERROR,
                  "Unexpected Arrow Field for ",
                  st.name());
          }

        case TIMESTAMP_LTZ:
          if (vector.getField().getChildren().isEmpty()) {
            // case when the scale of the timestamp is equal or smaller than millisecs since epoch
            return new BigIntToTimestampLTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 2) {
            // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
            // nanosecs
            return new TwoFieldStructToTimestampLTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected Arrow Field for ",
                st.name());
          }

        case TIMESTAMP_NTZ:
          if (vector.getField().getChildren().isEmpty()) {
            // case when the scale of the timestamp is equal or smaller than 7
            return new BigIntToTimestampNTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 2) {
            // when the timestamp is represent in two-field struct
            return new TwoFieldStructToTimestampNTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected Arrow Field for ",
                st.name());
          }

        case TIMESTAMP_TZ:
          if (vector.getField().getChildren().size() == 2) {
            // case when the scale of the timestamp is equal or smaller than millisecs since epoch
            return new TwoFieldStructToTimestampTZConverter(vector, idx, context);
          } else if (vector.getField().getChildren().size() == 3) {
            // case when the scale of the timestamp is larger than millisecs since epoch, e.g.,
            // nanosecs
            return new ThreeFieldStructToTimestampTZConverter(vector, idx, context);
          } else {
            throw new SnowflakeSQLLoggedException(
                session,
                ErrorCode.INTERNAL_ERROR.getMessageCode(),
                SqlState.INTERNAL_ERROR,
                "Unexpected SnowflakeType ",
                st.name());
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
    throw new SnowflakeSQLLoggedException(
        session,
        ErrorCode.INTERNAL_ERROR.getMessageCode(),
        SqlState.INTERNAL_ERROR,
        "Unexpected Arrow Field for ",
        type.toString());
  }

  public static ArrowVectorConverter initConverter(
      FieldVector vector, DataConversionContext context, int columnIndex)
      throws SnowflakeSQLException {
    return initConverter(vector, context, context.getSession(), columnIndex);
  }
}
