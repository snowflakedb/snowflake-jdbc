/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.id.State;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class RandomStateProvider implements StateProvider<String> {
  @Override
  public String getState() {
    return new State(256).getValue();
  }
}
