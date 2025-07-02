package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.SnowflakeUtil.getFieldMetadata;
import static net.snowflake.client.jdbc.SnowflakeUtil.getSnowflakeType;
import static net.snowflake.client.jdbc.SnowflakeUtil.isNullOrEmpty;
import static net.snowflake.client.jdbc.SnowflakeUtil.isVectorType;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Serializable;
import java.sql.Types;
import java.util.List;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

public class SnowflakeColumnMetadata implements Serializable {
  private static final long serialVersionUID = 1L;
  private String name;
  private String typeName;
  private int type;
  private boolean nullable;
  private int length;
  private int precision;
  private int scale;
  private boolean fixed;
  private SnowflakeType base;
  private List<FieldMetadata> fields;
  private String columnSrcTable;
  private String columnSrcSchema;
  private String columnSrcDatabase;

  private boolean isAutoIncrement;
  private int dimension; // vector type contains dimension

  @SnowflakeJdbcInternalApi
  public SnowflakeColumnMetadata(
      String name,
      int type,
      boolean nullable,
      int length,
      int precision,
      int scale,
      String typeName,
      boolean fixed,
      SnowflakeType base,
      List<FieldMetadata> fields,
      String columnSrcDatabase,
      String columnSrcSchema,
      String columnSrcTable,
      boolean isAutoIncrement,
      int dimension) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.length = length;
    this.precision = precision;
    this.scale = scale;
    this.typeName = typeName;
    this.fixed = fixed;
    this.base = base;
    this.fields = fields;
    this.columnSrcDatabase = columnSrcDatabase;
    this.columnSrcSchema = columnSrcSchema;
    this.columnSrcTable = columnSrcTable;
    this.isAutoIncrement = isAutoIncrement;
    this.dimension = dimension;
  }

  /**
   * @deprecated Use {@link SnowflakeColumnMetadata#SnowflakeColumnMetadata(String, int, boolean,
   *     int, int, int, String, boolean, SnowflakeType, List, String, String, String, boolean, int)}
   *     instead
   * @param name name
   * @param type type
   * @param nullable is nullable
   * @param length length
   * @param precision precision
   * @param scale scale
   * @param typeName type name
   * @param fixed is fixed
   * @param base SnowflakeType
   * @param columnSrcDatabase column source database
   * @param columnSrcSchema column source schema
   * @param columnSrcTable column source table
   * @param isAutoIncrement is auto-increment
   */
  @Deprecated
  public SnowflakeColumnMetadata(
      String name,
      int type,
      boolean nullable,
      int length,
      int precision,
      int scale,
      String typeName,
      boolean fixed,
      SnowflakeType base,
      String columnSrcDatabase,
      String columnSrcSchema,
      String columnSrcTable,
      boolean isAutoIncrement) {
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.length = length;
    this.precision = precision;
    this.scale = scale;
    this.typeName = typeName;
    this.fixed = fixed;
    this.base = base;
    this.columnSrcDatabase = columnSrcDatabase;
    this.columnSrcSchema = columnSrcSchema;
    this.columnSrcTable = columnSrcTable;
    this.isAutoIncrement = isAutoIncrement;
  }

  @SnowflakeJdbcInternalApi
  public SnowflakeColumnMetadata(
      JsonNode colNode, boolean jdbcTreatDecimalAsInt, SFBaseSession session)
      throws SnowflakeSQLLoggedException {
    this.name = colNode.path("name").asText();
    this.nullable = colNode.path("nullable").asBoolean();
    this.precision = colNode.path("precision").asInt();
    this.scale = colNode.path("scale").asInt();
    this.length = colNode.path("length").asInt();
    int dimension =
        colNode
            .path("dimension")
            .asInt(); // vector dimension when checking columns via connection.getMetadata
    int vectorDimension =
        colNode
            .path("vectorDimension")
            .asInt(); // dimension when checking columns via resultSet.getMetadata
    this.dimension = dimension > 0 ? dimension : vectorDimension;
    this.fixed = colNode.path("fixed").asBoolean();
    JsonNode udtOutputType = colNode.path("outputType");
    JsonNode extColTypeNameNode = colNode.path("extTypeName");
    String extColTypeName = null;
    if (!extColTypeNameNode.isMissingNode() && !isNullOrEmpty(extColTypeNameNode.asText())) {
      extColTypeName = extColTypeNameNode.asText();
    }
    String internalColTypeName = colNode.path("type").asText();
    List<FieldMetadata> fieldsMetadata =
        getFieldMetadata(jdbcTreatDecimalAsInt, internalColTypeName, colNode);

    int fixedColType = jdbcTreatDecimalAsInt && scale == 0 ? Types.BIGINT : Types.DECIMAL;
    ColumnTypeInfo columnTypeInfo =
        getSnowflakeType(
            internalColTypeName,
            extColTypeName,
            udtOutputType,
            session,
            fixedColType,
            !fieldsMetadata.isEmpty(),
            isVectorType(internalColTypeName));

    this.typeName = columnTypeInfo.getExtColTypeName();
    this.type = columnTypeInfo.getColumnType();
    this.base = columnTypeInfo.getSnowflakeType();
    this.fields = fieldsMetadata;
    this.columnSrcDatabase = colNode.path("database").asText();
    this.columnSrcSchema = colNode.path("schema").asText();
    this.columnSrcTable = colNode.path("table").asText();
    this.isAutoIncrement = colNode.path("isAutoIncrement").asBoolean();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public int getLength() {
    return length;
  }

  public void setLength(int length) {
    this.length = length;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public boolean isFixed() {
    return fixed;
  }

  public void setFixed(boolean fixed) {
    this.fixed = fixed;
  }

  public SnowflakeType getBase() {
    return this.base;
  }

  @SnowflakeJdbcInternalApi
  public List<FieldMetadata> getFields() {
    return fields;
  }

  @SnowflakeJdbcInternalApi
  public void setFields(List<FieldMetadata> fields) {
    this.fields = fields;
  }

  public String getColumnSrcTable() {
    return this.columnSrcTable;
  }

  public String getColumnSrcSchema() {
    return this.columnSrcSchema;
  }

  public String getColumnSrcDatabase() {
    return this.columnSrcDatabase;
  }

  public boolean isAutoIncrement() {
    return isAutoIncrement;
  }

  public void setAutoIncrement(boolean autoIncrement) {
    isAutoIncrement = autoIncrement;
  }

  @SnowflakeJdbcInternalApi
  public int getDimension() {
    return dimension;
  }

  public String toString() {
    StringBuilder sBuilder = new StringBuilder();

    sBuilder.append("name=").append(name);
    sBuilder.append(",typeName=").append(typeName);
    sBuilder.append(",type=").append(type);
    sBuilder.append(",nullable=").append(nullable);
    sBuilder.append(",length=").append(length);
    sBuilder.append(",precision=").append(precision);
    sBuilder.append(",scale=").append(scale);
    sBuilder.append(",fixed=").append(fixed);
    sBuilder.append(",database=").append(columnSrcDatabase);
    sBuilder.append(",schema=").append(columnSrcSchema);
    sBuilder.append(",table=").append(columnSrcTable);
    sBuilder.append((",isAutoIncrement=")).append(isAutoIncrement);
    sBuilder.append((",dimension=")).append(dimension);

    return sBuilder.toString();
  }
}
