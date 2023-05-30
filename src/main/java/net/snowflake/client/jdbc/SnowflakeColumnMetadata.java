/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import java.io.Serializable;

/**
 * @author jhuang
 */
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
  private String columnSrcTable;
  private String columnSrcSchema;
  private String columnSrcDatabase;

  private boolean isAutoIncrement;

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

    return sBuilder.toString();
  }
}
