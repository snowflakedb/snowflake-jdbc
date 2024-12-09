/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core.arrow;

import java.sql.SQLInput;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class StructObjectWrapper {
  private final String jsonString;
  private final SQLInput sqlInput;
  private final Object object;

  public StructObjectWrapper(String jsonString, SQLInput sqlInput) {
    this.jsonString = jsonString;
    this.sqlInput = sqlInput;
    this.object = null;
  }

  public StructObjectWrapper(String jsonString, SQLInput sqlInput, Object object) {
    this.jsonString = jsonString;
    this.sqlInput = sqlInput;
    this.object = object;
  }

  public String getJsonString() {
    return jsonString;
  }

  public SQLInput getSqlInput() {
    return sqlInput;
  }

  public Object getObject() {
    return object;
  }
}
