package net.snowflake.client.internal.core.arrow;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class StructObjectWrapper {
  private final String jsonString;
  private final Object object;

  public StructObjectWrapper(String jsonString, Object object) {
    this.jsonString = jsonString;
    this.object = object;
  }

  public String getJsonString() {
    return jsonString;
  }

  public Object getObject() {
    return object;
  }
}
