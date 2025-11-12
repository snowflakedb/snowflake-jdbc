package net.snowflake.client.internal.jdbc;

public class SnowflakeUseDPoPNonceException extends RuntimeException {

  private final String nonce;

  public SnowflakeUseDPoPNonceException(String nonce) {
    this.nonce = nonce;
  }

  public String getNonce() {
    return nonce;
  }
}
