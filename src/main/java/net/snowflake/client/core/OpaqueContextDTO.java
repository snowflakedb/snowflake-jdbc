package net.snowflake.client.core;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OpaqueContextDTO {
  @JsonInclude(JsonInclude.Include.NON_NULL)
  byte[] base64Data;

  @JsonCreator
  public OpaqueContextDTO(@JsonProperty("base64Data") byte[] base64Data) {
    this.base64Data = base64Data;
  }

  public byte[] getBase64Data() {
    return base64Data;
  }

  public void setBase64Data(byte[] base64Data) {
    this.base64Data = base64Data;
  }
}