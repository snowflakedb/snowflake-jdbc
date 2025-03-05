/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import java.util.Map;
import net.snowflake.client.jdbc.SnowflakeSQLException;

// Consumed by Snowsight
@SnowflakeJdbcInternalApi
class StmtInternal extends SFStatement {

  public StmtInternal(SFSession session) {
    super(session);
  }

  @Override
  public Object executeHelper(
      String sql,
      String mediaType,
      Map<String, ParameterBindingDTO> bindValues,
      boolean describeOnly,
      boolean internal,
      boolean asyncExec,
      ExecTimeTelemetryData execTimeData)
      throws SnowflakeSQLException, SFException {
    return super.executeHelper(
        sql, mediaType, bindValues, describeOnly, true, asyncExec, execTimeData);
  }
}
