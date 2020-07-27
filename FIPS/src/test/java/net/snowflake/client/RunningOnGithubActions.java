/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

/** Run tests on CI */
public class RunningOnGithubActions implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return System.getenv("GITHUB_ACTIONS") != null;
  }
}
