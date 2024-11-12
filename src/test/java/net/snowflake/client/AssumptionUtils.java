/*
 * Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.core.Constants;

public class AssumptionUtils {
  public static boolean isRunningOnGithubActionsMac() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null && Constants.getOS() == Constants.OS.MAC;
  }

  public static boolean isRunningOnJava8() {
    return systemGetProperty("java.version").startsWith("1.8.0");
  }

  public static boolean isRunningOnJava21() {
    return systemGetProperty("java.version").startsWith("21.");
  }

  public static boolean isRunningOnGithubAction() {
    return TestUtil.systemGetEnv("GITHUB_ACTIONS") != null;
  }

  public static boolean isNotRunningOnLinuxMac() {
    return Constants.getOS() != Constants.OS.LINUX && Constants.getOS() != Constants.OS.MAC;
  }
}
