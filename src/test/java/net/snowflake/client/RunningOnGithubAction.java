/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/** Run tests on CI */
public class RunningOnGithubAction implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null;
  }

  public static boolean isRunningOnGithubAction() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null;
  }
}
