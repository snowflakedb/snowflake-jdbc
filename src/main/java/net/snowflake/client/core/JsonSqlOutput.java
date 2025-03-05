package net.snowflake.client.core;

import static net.snowflake.client.core.FieldSchemaCreator.buildSchemaTypeAndNameOnly;
import static net.snowflake.client.core.FieldSchemaCreator.buildSchemaWithScaleAndPrecision;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLOutput;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.stream.Collectors;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.BindingParameterMetadata;
import net.snowflake.client.jdbc.SnowflakeColumn;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.jdbc.SnowflakeType;
import net.snowflake.client.jdbc.SnowflakeUtil;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.ThrowingTriCallable;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SFTime;
import net.snowflake.common.core.SFTimestamp;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class JsonSqlOutput implements SQLOutput {
  static final SFLogger logger = SFLoggerFactory.getLogger(JsonSqlOutput.class);
  private JSONObject json;
  private SQLData original;
  private SFBaseSession session;
  private Iterator<Field> fields;
  private BindingParameterMetadata schema;
  private TimeZone sessionTimezone;

  public JsonSqlOutput(SQLData original, SFBaseSession sfBaseSession) {
    this.original = original;
    this.session = sfBaseSession;
    this.sessionTimezone = getSessionTimezone(sfBaseSession);
    fields = getClassFields(original).iterator();
    schema = new BindingParameterMetadata("object");
    schema.setFields(new ArrayList<>());
    json = new JSONObject();
  }

  private TimeZone getSessionTimezone(SFBaseSession sfBaseSession) {
    String timeZoneName =
        (String) ResultUtil.effectiveParamValue(sfBaseSession.getCommonParameters(), "TIMEZONE");
    return TimeZone.getTimeZone(timeZoneName);
  }

  private static List<Field> getClassFields(SQLData original) {
    return Arrays.stream(original.getClass().getDeclaredFields())
        .filter(
            field ->
                !Modifier.isStatic(field.getModifiers())
                    && !Modifier.isTransient(field.getModifiers()))
        .collect(Collectors.toList());
  }

  @Override
  public void writeString(String value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema.getFields().add(FieldSchemaCreator.buildSchemaForText(fieldName, maybeColumn));
        }));
  }

  @Override
  public void writeBoolean(boolean value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema.getFields().add(buildSchemaTypeAndNameOnly(fieldName, "boolean", maybeColumn));
        }));
  }

  @Override
  public void writeByte(byte value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema
              .getFields()
              .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
        }));
  }

  @Override
  public void writeShort(short value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema
              .getFields()
              .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
        }));
  }

  @Override
  public void writeInt(int input) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, input);
          schema
              .getFields()
              .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
        }));
  }

  @Override
  public void writeLong(long value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema
              .getFields()
              .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
        }));
  }

  @Override
  public void writeFloat(float value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema.getFields().add(buildSchemaTypeAndNameOnly(fieldName, "real", maybeColumn));
        }));
  }

  @Override
  public void writeDouble(double value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema.getFields().add(buildSchemaTypeAndNameOnly(fieldName, "real", maybeColumn));
        }));
  }

  @Override
  public void writeBigDecimal(BigDecimal value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, value);
          schema
              .getFields()
              .add(
                  buildSchemaWithScaleAndPrecision(
                      fieldName, "fixed", value.scale(), 38, maybeColumn));
        }));
  }

  @Override
  public void writeBytes(byte[] value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(fieldName, new SFBinary(value).toHex());
          schema
              .getFields()
              .add(FieldSchemaCreator.buildSchemaForBytesType(fieldName, maybeColumn));
        }));
  }

  @Override
  public void writeDate(Date value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          json.put(
              fieldName,
              ResultUtil.getDateAsString(value, getDateTimeFormat("DATE_OUTPUT_FORMAT")));
          schema.getFields().add(buildSchemaTypeAndNameOnly(fieldName, "date", maybeColumn));
        }));
  }

  @Override
  public void writeTime(Time x) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          long nanosSinceMidnight = SfTimestampUtil.getTimeInNanoseconds(x);
          String result =
              ResultUtil.getSFTimeAsString(
                  SFTime.fromNanoseconds(nanosSinceMidnight),
                  9,
                  getDateTimeFormat("TIME_OUTPUT_FORMAT"));

          json.put(fieldName, result);
          schema
              .getFields()
              .add(buildSchemaWithScaleAndPrecision(fieldName, "time", 9, 0, maybeColumn));
        }));
  }

  @Override
  public void writeTimestamp(Timestamp value) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          String timestampSessionType =
              (String)
                  ResultUtil.effectiveParamValue(
                      session.getCommonParameters(), "CLIENT_TIMESTAMP_TYPE_MAPPING");
          SnowflakeType snowflakeType =
              SnowflakeType.fromString(
                  maybeColumn
                      .map(cl -> cl.type())
                      .filter(str -> !str.isEmpty())
                      .orElse(timestampSessionType));
          int columnType = snowflakeTypeToJavaType(snowflakeType);
          TimeZone timeZone = timeZoneDependOnType(snowflakeType, session, null);
          String timestampAsString =
              SnowflakeUtil.mapSFExceptionToSQLException(
                  () ->
                      ResultUtil.getSFTimestampAsString(
                          new SFTimestamp(value, timeZone),
                          columnType,
                          9,
                          getDateTimeFormat("TIMESTAMP_NTZ_OUTPUT_FORMAT"),
                          getDateTimeFormat("TIMESTAMP_LTZ_OUTPUT_FORMAT"),
                          getDateTimeFormat("TIMESTAMP_TZ_OUTPUT_FORMAT"),
                          session));

          json.put(fieldName, timestampAsString);
          schema
              .getFields()
              .add(
                  buildSchemaWithScaleAndPrecision(
                      fieldName, snowflakeType.name(), 9, 0, maybeColumn));
        }));
  }

  @Override
  public void writeCharacterStream(Reader x) throws SQLException {
    logger.debug(" Unsupported method writeCharacterStream(Reader x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeAsciiStream(InputStream x) throws SQLException {
    logger.debug("Unsupported method writeAsciiStream(InputStream x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeBinaryStream(InputStream x) throws SQLException {
    logger.debug("Unsupported method writeBinaryStream(InputStream x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeObject(SQLData sqlData) throws SQLException {
    withNextValue(
        ((json, fieldName, maybeColumn) -> {
          JsonSqlOutput jsonSqlOutput = new JsonSqlOutput(sqlData, session);
          sqlData.writeSQL(jsonSqlOutput);
          json.put(fieldName, jsonSqlOutput.getJsonObject());
          BindingParameterMetadata structSchema = jsonSqlOutput.getSchema();
          structSchema.setName(fieldName);
          schema.getFields().add(structSchema);
        }));
  }

  @Override
  public void writeRef(Ref x) throws SQLException {
    logger.debug("Unsupported method writeRef(Ref x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeBlob(Blob x) throws SQLException {
    logger.debug("Unsupported method writeBlob(Blob x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeClob(Clob x) throws SQLException {
    logger.debug("Unsupported method writeClob(Clob x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeStruct(Struct x) throws SQLException {
    logger.debug("Unsupported method writeStruct(Struct x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeArray(Array x) throws SQLException {
    logger.debug("Unsupported method writeArray(Array x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeURL(URL x) throws SQLException {
    logger.debug("Unsupported method writeURL(URL x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeNString(String x) throws SQLException {
    logger.debug("Unsupported method writeNString(String x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeNClob(NClob x) throws SQLException {
    logger.debug("Unsupported method writeNClob(NClob x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeRowId(RowId x) throws SQLException {
    logger.debug("Unsupported method writeRowId(RowId x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  @Override
  public void writeSQLXML(SQLXML x) throws SQLException {
    logger.debug("Unsupported method  writeSQLXML(SQLXML x)", false);
    throw new SnowflakeLoggedFeatureNotSupportedException(session);
  }

  public String getJsonString() {
    return json.toJSONString();
  }

  public JSONObject getJsonObject() {
    return json;
  }

  private void withNextValue(
      ThrowingTriCallable<JSONObject, String, Optional<SnowflakeColumn>, SQLException> action)
      throws SQLException {
    Field field = fields.next();
    String fieldName = field.getName();
    Optional<SnowflakeColumn> maybeColumn =
        Optional.ofNullable(field.getAnnotation(SnowflakeColumn.class));
    action.apply(json, fieldName, maybeColumn);
  }

  private SnowflakeDateTimeFormat getDateTimeFormat(String format) {
    String rawFormat = (String) session.getCommonParameters().get(format);
    if (rawFormat == null || rawFormat.isEmpty()) {
      rawFormat = (String) session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT");
    }
    SnowflakeDateTimeFormat formatter = SnowflakeDateTimeFormat.fromSqlFormat(rawFormat);
    return formatter;
  }

  public BindingParameterMetadata getSchema() {
    return schema;
  }

  private TimeZone timeZoneDependOnType(
      SnowflakeType snowflakeType, SFBaseSession session, TimeZone tz) {
    if (snowflakeType == SnowflakeType.TIMESTAMP_NTZ) {
      return null;
    } else if (snowflakeType == SnowflakeType.TIMESTAMP_LTZ) {
      return getSessionTimezone(session);
    } else if (snowflakeType == SnowflakeType.TIMESTAMP_TZ) {
      return Optional.ofNullable(tz).orElse(sessionTimezone);
    }
    return TimeZone.getDefault();
  }

  private int snowflakeTypeToJavaType(SnowflakeType snowflakeType) {
    if (snowflakeType == SnowflakeType.TIMESTAMP_NTZ) {
      return SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_NTZ;
    } else if (snowflakeType == SnowflakeType.TIMESTAMP_LTZ) {
      return SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_LTZ;
    }
    return SnowflakeUtil.EXTRA_TYPES_TIMESTAMP_TZ;
  }
}
