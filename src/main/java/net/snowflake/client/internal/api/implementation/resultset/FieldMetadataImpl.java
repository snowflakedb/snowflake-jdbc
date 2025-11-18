package net.snowflake.client.internal.api.implementation.resultset;

import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.api.resultset.FieldMetadata;
import net.snowflake.client.api.resultset.SnowflakeType;

/** Implementation of {@link FieldMetadata} for structured type field information. */
public class FieldMetadataImpl implements FieldMetadata {

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

  public FieldMetadataImpl(
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

  public FieldMetadataImpl() {
    this.fields = new ArrayList<>();
  }

  @Override
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  @Override
  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  @Override
  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  @Override
  public int getByteLength() {
    return byteLength;
  }

  public void setByteLength(int byteLength) {
    this.byteLength = byteLength;
  }

  @Override
  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  @Override
  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  @Override
  public boolean isFixed() {
    return fixed;
  }

  public void setFixed(boolean fixed) {
    this.fixed = fixed;
  }

  @Override
  public SnowflakeType getBase() {
    return base;
  }

  public void setBase(SnowflakeType base) {
    this.base = base;
  }

  @Override
  public List<FieldMetadata> getFields() {
    return fields;
  }

  public void setFields(List<FieldMetadata> fields) {
    this.fields = fields;
  }
}
