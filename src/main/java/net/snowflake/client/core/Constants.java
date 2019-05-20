/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

/*
 * Constants used in JDBC implementation
 */
public final class Constants
{
  // Session expired error code as returned from Snowflake
  public static final int SESSION_EXPIRED_GS_CODE = 390112;

  // Session gone error code as returned from Snowflake
  public static final int SESSION_GONE = 390111;

  public enum OS
  {
    WINDOWS,
    LINUX,
    MAC,
    SOLARIS
  }

  private static OS os = null;

  public static synchronized OS getOS()
  {
    if (os == null)
    {
      String operSys = System.getProperty("os.name").toLowerCase();
      if (operSys.contains("win"))
      {
        os = OS.WINDOWS;
      }
      else if (operSys.contains("nix") || operSys.contains("nux")
               || operSys.contains("aix"))
      {
        os = OS.LINUX;
      }
      else if (operSys.contains("mac"))
      {
        os = OS.MAC;
      }
      else if (operSys.contains("sunos"))
      {
        os = OS.SOLARIS;
      }
    }
    return os;
  }
}
