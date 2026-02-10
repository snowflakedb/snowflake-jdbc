package net.snowflake.client.internal.driver;

import net.snowflake.client.api.exception.ErrorCode;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.common.core.SqlState;

/**
 * Manages Snowflake JDBC driver version information.
 *
 * <p>This class is responsible for parsing and providing access to the driver's version numbers
 * (major, minor, and patch). The version is parsed from a version string in the format
 * "major.minor.patch".
 *
 * <p>This class is thread-safe and immutable.
 */
public final class DriverVersion {
  private static final String implementVersion = "4.0.2";

  private final int major;
  private final int minor;
  private final long patch;
  private final String fullVersion;

  private static final DriverVersion INSTANCE = parseFromStaticVersion();

  private DriverVersion(int major, int minor, long patch, String fullVersion) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
    this.fullVersion = fullVersion;
  }

  /**
   * Gets the singleton instance of DriverVersion.
   *
   * @return the driver version instance
   */
  public static DriverVersion getInstance() {
    return INSTANCE;
  }

  /**
   * Parses a version string in the format "major.minor.patch".
   *
   * @param versionString the version string to parse
   * @return a DriverVersion instance
   * @throws IllegalArgumentException if the version string is invalid
   */
  public static DriverVersion parse(String versionString) throws IllegalArgumentException {
    if (versionString == null || versionString.isEmpty()) {
      throw new IllegalArgumentException("Version string cannot be null or empty");
    }

    String[] parts = versionString.split("\\.");
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          "Invalid version format: " + versionString + ". Expected: major.minor.patch");
    }

    try {
      int major = Integer.parseInt(parts[0]);
      int minor = Integer.parseInt(parts[1]);
      long patch = Long.parseLong(parts[2]);
      return new DriverVersion(major, minor, patch, versionString);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid version numbers in: " + versionString, e);
    }
  }

  /**
   * Parses the version from the static version string defined in the driver. This is called during
   * static initialization.
   *
   * @return a DriverVersion instance
   */
  private static DriverVersion parseFromStaticVersion() {
    try {
      if (implementVersion != null && !implementVersion.isEmpty()) {
        return parse(implementVersion);
      } else {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            /*session = */ null,
            "Snowflake JDBC Version is not set. "
                + "Ensure static version string was initialized.");
      }
    } catch (IllegalArgumentException ex) {
      // Re-throw as SnowflakeSQLException for consistency with original code
      throw new RuntimeException(
          new SnowflakeSQLException(
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              /*session = */ null,
              "Invalid Snowflake JDBC Version: " + implementVersion));
    } catch (SnowflakeSQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Gets the major version number.
   *
   * @return the major version
   */
  public int getMajor() {
    return major;
  }

  /**
   * Gets the minor version number.
   *
   * @return the minor version
   */
  public int getMinor() {
    return minor;
  }

  /**
   * Gets the patch version number.
   *
   * @return the patch version
   */
  public long getPatch() {
    return patch;
  }

  /**
   * Gets the full version string in the format "major.minor.patch".
   *
   * @return the full version string
   */
  public String getFullVersion() {
    return fullVersion;
  }

  @Override
  public String toString() {
    return fullVersion;
  }
}
