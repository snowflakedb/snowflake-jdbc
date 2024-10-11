/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc.cloud.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SnowflakeS3ClientTest {

  @Test
  public void shouldDetermineDomainForRegion() {
    Assertions.assertEquals(
        "amazonaws.com", SnowflakeS3Client.getDomainSuffixForRegionalUrl("us-east-1"));
    Assertions.assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("cn-northwest-1"));
    Assertions.assertEquals(
        "amazonaws.com.cn", SnowflakeS3Client.getDomainSuffixForRegionalUrl("CN-NORTHWEST-1"));
  }
}
