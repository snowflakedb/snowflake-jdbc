package net.snowflake.client.jdbc.cloud.storage.floe;

public interface FloeDecryptor extends AutoCloseable {
  byte[] processSegment(byte[] ciphertext);

  boolean isClosed();
}
