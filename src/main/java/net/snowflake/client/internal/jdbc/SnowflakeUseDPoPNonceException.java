package net.snowflake.client.internal.jdbc;

import net.snowflake.client.internal.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class SnowflakeUseDPoPNonceException extends RuntimeException {

  private final String nonce;

  public SnowflakeUseDPoPNonceException(String nonce) {
    this.nonce = nonce;
  }

  public String getNonce() {
    return nonce;
  }
}
