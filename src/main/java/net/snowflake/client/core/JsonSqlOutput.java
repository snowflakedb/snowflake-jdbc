/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.core;

import static net.snowflake.client.core.FieldSchemaCreator.buildSchemaWithScaleAndPrecision;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Field;
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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.TimeZone;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.BindingParameterMetadata;
import net.snowflake.client.jdbc.SnowflakeColumn;
import net.snowflake.client.util.ThrowingTriCallable;
import net.snowflake.common.core.SFBinary;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class JsonSqlOutput implements SQLOutput {
  public static final int MAX_TEXT_COLUMN_SIZE = 134217728;
  private JSONObject json = new JSONObject();
  private SQLData original;
  private SFBaseSession session;
  private Iterator<Field> fields;
  private BindingParameterMetadata schema;
  private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss.");

  public JsonSqlOutput(SQLData original) {
    this(original, null);
  }

  public JsonSqlOutput(SQLData original, SFBaseSession sfBaseSession) {
    this.original = original;
    this.session = sfBaseSession;
    fields = Arrays.stream(original.getClass().getDeclaredFields()).iterator();
    schema = new BindingParameterMetadata("object");
    schema.setFields(new ArrayList<>());
  }

  @Override
  public void writeString(String value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(FieldSchemaCreator.buildSchemaForVarchar(fieldName, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBoolean(boolean value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(
                    FieldSchemaCreator.buildSchemaTypeAndNameOnly(
                        fieldName, "boolean", maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeByte(byte value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeShort(short value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInt(int input) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, input);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeLong(long value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "fixed", 0, 38, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeFloat(float value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "real", 0, 0, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeDouble(double value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "real", 0, 0, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBigDecimal(BigDecimal value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, value);
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "real", 0, 0, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBytes(byte[] value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, new SFBinary(value).toHex());
            schema
                .getFields()
                .add(FieldSchemaCreator.buildSchemaForBytesType(fieldName, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeDate(Date value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, ResultUtil.getDateAsString(value, getDateTimeFormat(null)));
            schema
                .getFields()
                .add(FieldSchemaCreator.buildSchemaTypeAndNameOnly(fieldName, "date", maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeTime(Time value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, timeFormat.format(value));
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "time", 9, 0, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeTimestamp(Timestamp value) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            json.put(fieldName, getDateTimeFormat(null).format(value, TimeZone.getDefault(), 0));
            schema
                .getFields()
                .add(buildSchemaWithScaleAndPrecision(fieldName, "timestamp", 9, 0, maybeColumn));
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeCharacterStream(Reader x) throws SQLException {}

  @Override
  public void writeAsciiStream(InputStream x) throws SQLException {}

  @Override
  public void writeBinaryStream(InputStream x) throws SQLException {}

  @Override
  public void writeObject(SQLData sqlData) throws SQLException {
    try {
      withNextValue(
          ((json, fieldName, maybeColumn) -> {
            JsonSqlOutput jsonSqlOutput = new JsonSqlOutput(sqlData);
            sqlData.writeSQL(jsonSqlOutput);
            json.put(fieldName, jsonSqlOutput.getJsonObject());
            BindingParameterMetadata structSchema = jsonSqlOutput.getSchema();
            structSchema.setName(fieldName);
            schema.getFields().add(structSchema);
          }));
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeRef(Ref x) throws SQLException {}

  @Override
  public void writeBlob(Blob x) throws SQLException {}

  @Override
  public void writeClob(Clob x) throws SQLException {}

  @Override
  public void writeStruct(Struct x) throws SQLException {}

  @Override
  public void writeArray(Array x) throws SQLException {}

  @Override
  public void writeURL(URL x) throws SQLException {}

  @Override
  public void writeNString(String x) throws SQLException {}

  @Override
  public void writeNClob(NClob x) throws SQLException {}

  @Override
  public void writeRowId(RowId x) throws SQLException {}

  @Override
  public void writeSQLXML(SQLXML x) throws SQLException {}

  public String getJsonString() {
    return json.toJSONString();
  }

  public JSONObject getJsonObject() {
    return json;
  }

  private void withNextValue(
      ThrowingTriCallable<JSONObject, String, Optional<SnowflakeColumn>, SQLException> action)
      throws Throwable {
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
}
