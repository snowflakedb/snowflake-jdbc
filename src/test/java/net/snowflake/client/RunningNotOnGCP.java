/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/** Run tests only on specified cloud provider or ignore */
public class RunningNotOnGCP implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    String cloudProvider = TestUtil.systemGetEnv("JDBC_TEST_CLOUD_PROVIDER");
    return cloudProvider != null && !cloudProvider.equalsIgnoreCase("GCP");
  }
}
