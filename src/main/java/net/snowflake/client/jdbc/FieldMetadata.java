package net.snowflake.client.jdbc;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

public class FieldMetadata {

  private String name;
  private String typeName;
  private int type;
  private boolean nullable;

  private int byteLength;

  private int precision;
  private int scale;
  private boolean fixed;
  private SnowflakeType base;
  private List<FieldMetadata> fields;

  public FieldMetadata(
      String name,
      String typeName,
      int type,
      boolean nullable,
      int byteLength,
      int precision,
      int scale,
      boolean fixed,
      SnowflakeType base,
      List<FieldMetadata> fields) {
    this.name = name;
    this.typeName = typeName;
    this.type = type;
    this.nullable = nullable;
    this.byteLength = byteLength;
    this.precision = precision;
    this.scale = scale;
    this.fixed = fixed;
    this.base = base;
    this.fields = fields;
  }

  @SnowflakeJdbcInternalApi
  public FieldMetadata() {
    this.fields = new ArrayList<>();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
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

  public int getByteLength() {
    return byteLength;
  }

  public void setByteLength(int byteLength) {
    this.byteLength = byteLength;
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

  public boolean isFixed() {
    return fixed;
  }

  public void setFixed(boolean fixed) {
    this.fixed = fixed;
  }

  public SnowflakeType getBase() {
    return base;
  }

  public void setBase(SnowflakeType base) {
    this.base = base;
  }

  public List<FieldMetadata> getFields() {
    return fields;
  }

  public void setFields(List<FieldMetadata> fields) {
    this.fields = fields;
  }
}
