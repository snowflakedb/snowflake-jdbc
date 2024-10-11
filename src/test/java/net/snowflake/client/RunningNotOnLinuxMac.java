package net.snowflake.client;

import net.snowflake.client.core.Constants;

@Deprecated
public class RunningNotOnLinuxMac {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.LINUX && Constants.getOS() != Constants.OS.MAC;
  }

  public static boolean isNotRunningOnLinuxMac() {
    return Constants.getOS() != Constants.OS.LINUX && Constants.getOS() != Constants.OS.MAC;
  }
}
