package net.snowflake.client.jdbc;

import net.snowflake.client.core.SnowflakeJdbcInternalApi;

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
