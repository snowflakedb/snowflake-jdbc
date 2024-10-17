package net.snowflake.client.core;

import static net.snowflake.client.core.FieldSchemaCreator.buildBindingSchemaForType;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.snowflake.client.jdbc.BindingParameterMetadata;

@SnowflakeJdbcInternalApi
public class SfSqlArray implements Array {

  private JsonNode input;
  private int baseType;
  private Object elements;

  public SfSqlArray(JsonNode input, int baseType, Object elements) {
    this.input = input;
    this.baseType = baseType;
    this.elements = elements;
  }

  public SfSqlArray(int baseType, Object elements) {
    this.input = null;
    this.baseType = baseType;
    this.elements = elements;
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

  public Object getElements() {
    return elements;
  }

  public BindingParameterMetadata getSchema() throws SQLException {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType("array")
        .withFields(Arrays.asList(buildBindingSchemaForType(getBaseType(), false)))
        .build();
  }

  public BindingParameterMetadata getSchema(List<BindingParameterMetadata> fields)
      throws SQLException {
    return BindingParameterMetadata.BindingParameterMetadataBuilder.bindingParameterMetadata()
        .withType("array")
        .withFields(fields)
        .build();
  }

  public JsonNode getInput() {
    return input;
  }
}
