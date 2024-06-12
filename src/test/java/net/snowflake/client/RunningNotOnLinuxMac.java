package net.snowflake.client;

import net.snowflake.client.core.Constants;

public class RunningNotOnLinuxMac implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.LINUX && Constants.getOS() != Constants.OS.MAC;
  }

  public static boolean isNotRunningOnLinuxMac() {
    return Constants.getOS() != Constants.OS.LINUX && Constants.getOS() != Constants.OS.MAC;
  }
}
