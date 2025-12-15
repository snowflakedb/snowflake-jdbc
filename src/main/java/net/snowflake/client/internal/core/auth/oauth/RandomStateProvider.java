package net.snowflake.client.internal.core.auth.oauth;

import com.nimbusds.oauth2.sdk.id.State;

public class RandomStateProvider implements StateProvider<String> {

  private static final int STATE_BYTE_SIZE = 256;

  @Override
  public String getState() {
    return new State(STATE_BYTE_SIZE).getValue();
  }
}
