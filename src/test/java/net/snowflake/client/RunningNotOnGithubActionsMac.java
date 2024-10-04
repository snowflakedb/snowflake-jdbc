/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client;

import net.snowflake.client.core.Constants;

@Deprecated
public class RunningNotOnGithubActionsMac implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return isRunningOnGithubActionsMac();
  }

  public static boolean isRunningOnGithubActionsMac() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null && Constants.getOS() == Constants.OS.MAC;
  }
}
