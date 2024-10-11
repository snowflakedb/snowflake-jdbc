/*
 * Copyright (c) 2012-2024 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/** Run tests only on specified cloud provider or ignore */
@Deprecated
public class RunningNotOnAzure {
  public boolean isSatisfied() {
    String cloudProvider = TestUtil.systemGetEnv("CLOUD_PROVIDER");
    return cloudProvider != null && !cloudProvider.equalsIgnoreCase("Azure");
  }
}
