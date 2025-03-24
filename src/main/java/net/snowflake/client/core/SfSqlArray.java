package net.snowflake.client.core;

import static net.snowflake.client.core.FieldSchemaCreator.buildBindingSchemaForType;
import static net.snowflake.client.core.FieldSchemaCreator.logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;
import net.snowflake.client.jdbc.BindingParameterMetadata;
import net.snowflake.client.jdbc.SnowflakeUtil;

@SnowflakeJdbcInternalApi
public class SfSqlArray implements Array {

  private String text;
  private int baseType;
  private Object elements;
  private String jsonStringFromElements;

  public SfSqlArray(String text, int baseType, Object elements) {
    this.text = text;
    this.baseType = baseType;
    this.elements = elements;
  }

  public SfSqlArray(int baseType, Object elements) {
    this(null, baseType, elements);
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

  private static String buildJsonStringFromElements(Object elements) throws SQLException {
    try {
      return SnowflakeUtil.mapJson(elements);
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
