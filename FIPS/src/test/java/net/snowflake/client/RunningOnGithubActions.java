/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

<<<<<<< HEAD
/** Run tests on CI */
public class RunningOnGithubActions implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null;
=======
/**
 * Run tests on CI
 */
public class RunningOnGithubActions implements ConditionalIgnoreRule.IgnoreCondition
{
  public boolean isSatisfied()
  {
    return System.getenv("GITHUB_ACTIONS") != null;
>>>>>>> parent of d33a7811... SNOW-175613 change coding style to Google Java Format (#291)
  }
}
