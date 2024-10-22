/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class SnowflakeS3ClientTest {

  @Test
  public void shouldDetermineDomainForRegion() {
    assertEquals("amazonaws.com", SnowflakeS3Client.getDomainSuffixForRegionalUrl("us-east-1"));
    assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("cn-northwest-1"));
    assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("CN-NORTHWEST-1"));
  }
}
