package net.snowflake.client.core;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.jdbc.SnowflakeUtil;

/*
 * Constants used in JDBC implementation
 */
public final class Constants {
  // Session expired error code as returned from Snowflake
  public static final int SESSION_EXPIRED_GS_CODE = 390112;

  // Cloud storage credentials expired error code
  public static final int CLOUD_STORAGE_CREDENTIALS_EXPIRED = 240001;

  // Session gone error code as returned from Snowflake
  public static final int SESSION_GONE = 390111;

  // Error code for all invalid id token cases during login request
  public static final int ID_TOKEN_INVALID_LOGIN_REQUEST_GS_CODE = 390195;

  public static final int OAUTH_ACCESS_TOKEN_EXPIRED_GS_CODE = 390318;

  public static final int OAUTH_ACCESS_TOKEN_INVALID_GS_CODE = 390303;

  // Error message for IOException when no space is left for GET
  public static final String NO_SPACE_LEFT_ON_DEVICE_ERR = "No space left on device";

  public enum OS {
    WINDOWS,
    LINUX,
    MAC,
    SOLARIS
  }

  public enum Architecture {
    X86_64("x86_64"),
    AARCH64("aarch64"),
    PPC64("ppc64"),
    X86("x86"),
    UNKNOWN("unknown");

    private final String identifier;

    Architecture(String identifier) {
      this.identifier = identifier;
    }

    public String getIdentifier() {
      return identifier;
    }
  }

  private static OS os = null;
  private static Architecture architecture = null;
  private static Boolean isAix = null;

  public static synchronized OS getOS() {
    if (os == null) {
      String operSys = systemGetProperty("os.name").toLowerCase();
      if (operSys.contains("win")) {
        os = OS.WINDOWS;
      } else if (operSys.contains("nix") || operSys.contains("nux") || operSys.contains("aix")) {
        os = OS.LINUX;
      } else if (operSys.contains("mac")) {
        os = OS.MAC;
      } else if (operSys.contains("sunos")) {
        os = OS.SOLARIS;
      }
    }
    return os;
  }

  public static synchronized Architecture getArchitecture() {
    if (architecture == null) {
      architecture = Architecture.UNKNOWN;
      String osArch = systemGetProperty("os.arch");
      if (!SnowflakeUtil.isNullOrEmpty(osArch)) {
        osArch = osArch.toLowerCase();
        if (osArch.contains("amd64") || osArch.contains("x86_64")) {
          architecture = Architecture.X86_64;
        } else if (osArch.contains("aarch64") || osArch.contains("arm64")) {
          architecture = Architecture.AARCH64;
        } else if (osArch.contains("ppc64")) {
          architecture = Architecture.PPC64;
        } else if (osArch.contains("x86") || osArch.contains("i386") || osArch.contains("i686")) {
          architecture = Architecture.X86;
        }
      }
    }
    return architecture;
  }

  public static boolean isAix() {
    if (isAix == null) {
      String osName = systemGetProperty("os.name");
      isAix = osName != null && osName.toLowerCase().contains("aix");
    }
    return isAix;
  }

  public static void clearOSForTesting() {
    os = null;
    architecture = null;
  }

  public static final int MB = 1024 * 1024;
  public static final long GB = 1024 * 1024 * 1024;
}
