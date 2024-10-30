package net.snowflake.client.core.arrow;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class StructObject {
  private final String stringJson;
  private final Object object;

  public StructObject(String stringJson, Object object) {
    this.stringJson = stringJson;
    this.object = object;
  }

  public String getStringJson() {
    return stringJson;
  }

  public Object getObject() {
    return object;
  }
}
