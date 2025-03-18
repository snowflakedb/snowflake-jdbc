package net.snowflake.client.core.auth.oauth;

import com.nimbusds.oauth2.sdk.id.State;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class RandomStateProvider implements StateProvider<String> {

  private static final int STATE_BYTE_SIZE = 256;

  @Override
  public String getState() {
    return new State(STATE_BYTE_SIZE).getValue();
  }
}
