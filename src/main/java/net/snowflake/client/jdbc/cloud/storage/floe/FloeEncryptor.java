package net.snowflake.client.jdbc.cloud.storage.floe;

public interface FloeEncryptor extends AutoCloseable {
  byte[] processSegment(byte[] plaintext);

  byte[] processLastSegment(byte[] plaintext);

  byte[] getHeader();

  boolean isClosed();
}
