package net.snowflake.client;

import net.snowflake.client.core.Constants;

@Deprecated
public class RunningNotOnWin {
  public boolean isSatisfied() {
    return Constants.getOS() != Constants.OS.WINDOWS;
  }
}
