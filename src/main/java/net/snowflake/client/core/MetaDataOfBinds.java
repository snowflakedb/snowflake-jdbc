package net.snowflake.client.core;

import java.io.Serializable;

/**
 * Class that creates constructor used for storing information about a binding parameter's metadata.
 * Each instantiation of a MetaDataOfBinds object corresponds to one binding parameter; an arraylist
 * of MetaDataOfBinds corresponds to a list of binding parameters in a prepared statement.
 */
public class MetaDataOfBinds implements Serializable {
  private static final long serialVersionUID = 1L;

  private int precision;

  private boolean nullable;

  private int scale;

  private int byteLength;

  private int length;

  private String name;

  private String type;

  public MetaDataOfBinds(int prec, boolean n, int sc, int bL, int len, String name, String type) {
    this.precision = prec;
    this.nullable = n;
    this.scale = sc;
    this.byteLength = bL;
    this.length = len;
    this.name = name;
    this.type = type;
  }

  public int getPrecision() {
    return this.precision;
  }

  public boolean isNullable() {
    return this.nullable;
  }

  public int getScale() {
    return this.scale;
  }

  public int getByteLength() {
    return this.byteLength;
  }

  public int getLength() {
    return this.length;
  }

  public String getName() {
    return this.name;
  }

  public String getTypeName() {
    return this.type;
  }
}
