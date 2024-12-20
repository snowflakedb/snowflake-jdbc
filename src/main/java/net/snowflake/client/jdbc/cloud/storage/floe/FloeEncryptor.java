package net.snowflake.client.jdbc.cloud.storage.floe;

public interface FloeEncryptor extends SegmentProcessor {
  byte[] getHeader();
}
