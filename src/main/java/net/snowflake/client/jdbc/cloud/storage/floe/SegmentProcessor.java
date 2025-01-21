package net.snowflake.client.jdbc.cloud.storage.floe;

interface SegmentProcessor {
  byte[] processSegment(byte[] input);
  byte[] processLastSegment(byte[] input);
}
