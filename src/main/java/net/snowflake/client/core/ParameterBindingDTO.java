package net.snowflake.client.core;

import net.snowflake.client.jdbc.BindingParameterMetadata;

/** This class represents a binding object passed to server side. */
public class ParameterBindingDTO {
  /** Type of binding */
  private String type;

  private String fmt;
  private BindingParameterMetadata schema;

  /** Value is a String object if it's a single bind, otherwise is an array of String */
  private Object value;

  public ParameterBindingDTO(
      String fmt, String type, Object value, BindingParameterMetadata schema) {
    this.fmt = fmt;
    this.type = type;
    this.value = value;
    this.schema = schema;
  }

  public ParameterBindingDTO(String fmt, String type, Object value) {
    this(fmt, type, value, null);
  }

  public ParameterBindingDTO(String type, Object value) {
    this(null, type, value, null);
  }

  public Object getValue() {
    return value;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public String getFmt() {
    return fmt;
  }

  public void setFmt(String fmt) {
    this.fmt = fmt;
  }

  public BindingParameterMetadata getSchema() {
    return schema;
  }

  public void setSchema(BindingParameterMetadata schema) {
    this.schema = schema;
  }
}
