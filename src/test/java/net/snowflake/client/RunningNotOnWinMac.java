package net.snowflake.client;

import net.snowflake.client.core.Constants;

public class RunningNotOnWinMac implements ConditionalIgnoreRule.IgnoreCondition {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.MAC && Constants.getOS() != Constants.OS.WINDOWS;
  }
}
