package net.snowflake.client.jdbc;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class BindingParameterMetadata {
  private String type;
  private String name;
  private Integer length;
  private Integer byteLength;
  private Integer precision;
  private Integer scale;

  private boolean nullable = true;
  private List<BindingParameterMetadata> fields;

  public BindingParameterMetadata(String type) {
    this.type = type;
  }

  public BindingParameterMetadata(String type, String name) {
    this.type = type;
    this.name = name;
  }

  public BindingParameterMetadata(
      String type,
      String name,
      Integer length,
      Integer byteLength,
      Integer precision,
      Integer scale,
      Boolean nullable) {
    this.type = type;
    this.name = name;
    this.length = length;
    this.byteLength = byteLength;
    this.precision = precision;
    this.scale = scale;
    this.nullable = nullable;
  }

  public BindingParameterMetadata() {}

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getLength() {
    return length;
  }

  public void setLength(Integer length) {
    this.length = length;
  }

  public Integer getByteLength() {
    return byteLength;
  }

  public void setByteLength(Integer byteLength) {
    this.byteLength = byteLength;
  }

  public Integer getPrecision() {
    return precision;
  }

  public void setPrecision(Integer precision) {
    this.precision = precision;
  }

  public Integer getScale() {
    return scale;
  }

  public void setScale(Integer scale) {
    this.scale = scale;
  }

  public Boolean isNullable() {
    return nullable;
  }

  public void setNullable(Boolean nullable) {
    this.nullable = nullable;
  }

  public List<BindingParameterMetadata> getFields() {
    return fields;
  }

  public void setFields(List<BindingParameterMetadata> fields) {
    this.fields = fields;
  }

  public static class BindingParameterMetadataBuilder {
    private BindingParameterMetadata bindingParameterMetadata;

    private BindingParameterMetadataBuilder() {
      bindingParameterMetadata = new BindingParameterMetadata();
    }

    public BindingParameterMetadataBuilder withType(String type) {
      bindingParameterMetadata.type = type;
      return this;
    }

    public BindingParameterMetadataBuilder withName(String name) {
      bindingParameterMetadata.name = name;
      return this;
    }

    public BindingParameterMetadataBuilder withLength(Integer length) {
      bindingParameterMetadata.length = length;
      return this;
    }

    public BindingParameterMetadataBuilder withByteLength(Integer byteLength) {
      bindingParameterMetadata.byteLength = byteLength;
      return this;
    }

    public BindingParameterMetadataBuilder withPrecision(Integer precision) {
      bindingParameterMetadata.precision = precision;
      return this;
    }

    public BindingParameterMetadataBuilder withScale(Integer scale) {
      bindingParameterMetadata.scale = scale;
      return this;
    }

    public BindingParameterMetadataBuilder withNullable(Boolean nullable) {
      bindingParameterMetadata.nullable = nullable;
      return this;
    }

    public BindingParameterMetadataBuilder withFields(List<BindingParameterMetadata> fields) {
      bindingParameterMetadata.fields = fields;
      return this;
    }

    public static BindingParameterMetadataBuilder bindingParameterMetadata() {
      return new BindingParameterMetadataBuilder();
    }

    public BindingParameterMetadata build() {
      return bindingParameterMetadata;
    }
  }
}
