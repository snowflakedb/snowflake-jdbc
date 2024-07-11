/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/** This class represents a binding object passed to server side Created by hyu on 6/15/17. */
public class ParameterBindingDTO {
  /** Type of binding */
  private String type;

  /** Value is a String object if it's a single bind, otherwise is an array of String */
  private Object value;

  public ParameterBindingDTO(String type, Object value) {
    this.type = type;
    this.value = value;
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
}
