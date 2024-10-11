package net.snowflake.client;

import net.snowflake.client.core.Constants;

@Deprecated
public class RunningNotOnWinMac {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.MAC && Constants.getOS() != Constants.OS.WINDOWS;
  }
}
