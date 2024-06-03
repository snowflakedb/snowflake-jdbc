/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import net.snowflake.client.jdbc.SnowflakeUtil;

/** Run tests on CI */
public class RunningOnGithubActions implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return SnowflakeUtil.systemGetEnv("GITHUB_ACTIONS") != null;
  }
}
