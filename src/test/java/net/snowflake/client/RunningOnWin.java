package net.snowflake.client;

import net.snowflake.client.core.Constants;

@Deprecated
public class RunningOnWin {
  public boolean isSatisfied() {
    return Constants.getOS() == Constants.OS.WINDOWS;
  }
}
