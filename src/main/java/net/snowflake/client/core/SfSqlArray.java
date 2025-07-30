package net.snowflake.client.core;

import static net.snowflake.client.core.FieldSchemaCreator.buildBindingSchemaForType;
import static net.snowflake.client.core.FieldSchemaCreator.logger;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Map;
import net.snowflake.client.jdbc.BindingParameterMetadata;
import net.snowflake.common.core.SnowflakeDateTimeFormat;

@SnowflakeJdbcInternalApi
public class SfSqlArray implements Array {

  private final String text;
  private final int baseType;
  private final Object elements;
  private String jsonStringFromElements;
  private final ObjectMapper objectMapper;

  public SfSqlArray(String text, int baseType, Object elements, SFBaseSession session) {
    this.text = text;
    this.baseType = baseType;
    this.elements = elements;
    SnowflakeDateTimeFormat timestampFormat =
        SnowflakeDateTimeFormat.fromSqlFormat(
            String.valueOf(session.getCommonParameters().get("TIMESTAMP_OUTPUT_FORMAT")));
    SnowflakeDateTimeFormat timeFormat =
        SnowflakeDateTimeFormat.fromSqlFormat(
            String.valueOf(session.getCommonParameters().get("TIME_OUTPUT_FORMAT")));
    objectMapper =
        ObjectMapperFactory.getObjectMapper()
            .setDateFormat(
                new SimpleDateFormat(
                    String.valueOf(session.getCommonParameters().get("DATE_OUTPUT_FORMAT"))))
            .registerModule(
                new SimpleModule()
                    .addSerializer(
                        Time.class,
                        new StdSerializer<Time>(Time.class) {
                          @Override
                          public void serialize(
                              Time value, JsonGenerator gen, SerializerProvider provider)
                              throws IOException {
                            gen.writeString(timeFormat.format(value, "UTC"));
                          }
                        }))
            .registerModule(
                new SimpleModule()
                    .addSerializer(
                        Timestamp.class,
                        new StdSerializer<Timestamp>(Timestamp.class) {
                          @Override
                          public void serialize(
                              Timestamp value, JsonGenerator gen, SerializerProvider provider)
                              throws IOException {
                            gen.writeString(timestampFormat.format(value, "UTC"));
                          }
                        }));
  }

  public SfSqlArray(int baseType, Object elements, SFBaseSession session) {
    this(null, baseType, elements, session);
  }

  @Override
  public String getBaseTypeName() throws SQLException {
    return JDBCType.valueOf(baseType).getName();
  }

  @Override
  public int getBaseType() throws SQLException {
    return baseType;
  }

  @Override
  public Object getArray() throws SQLException {
    return elements;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray(Map<String, Class<?>> map)");
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException("getArray(long index, int count)");
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getArray(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSet(Map<String, Class<?>> map)");
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSet(long index, int count)");
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "getResultSet(long index, int count, Map<String, Class<?>> map)");
  }

  @Override
  public void free() throws SQLException {}

  public String getText() {
    if (text == null) {
      logger.warn("Text field wasn't initialized. Should never happen.");
    }
    return text;
  }

  public String getJsonString() throws SQLException {
    if (jsonStringFromElements == null) {
      jsonStringFromElements = buildJsonStringFromElements(elements);
    }

    return jsonStringFromElements;
  }

  private String buildJsonStringFromElements(Object elements) throws SQLException {
    try {
      return objectMapper.writeValueAsString(elements);
    } catch (JsonProcessingException e) {
      throw new SQLException("There is exception during array to json string.", e);
    }
  }

  public BindingParameterMetadata getSchema() throws SQLException {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType("array")
        .withFields(Arrays.asList(buildBindingSchemaForType(getBaseType(), false)))
        .build();
  }
}
