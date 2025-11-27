package net.snowflake.client.internal.core.minicore;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.core.Constants;
import net.snowflake.client.core.Constants.Architecture;
import net.snowflake.client.core.Constants.OS;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class MinicorePlatform {
  private static final String MINICORE_VERSION = "0.0.1_SNAPSHOT";
  private static final String MINICORE_GIT_HASH = "47a8f3d456b27d6253cd8020a80dd6fc885169a3";
  private static final String LIBRARY_BASE_NAME = "sf_mini_core";

  private final OS os;
  private final Architecture architecture;
  private final String osName;
  private final String osArch;
  private final String resourcePath;
  private final boolean supported;

  public MinicorePlatform() {
    this.osName = systemGetProperty("os.name");
    this.osArch = systemGetProperty("os.arch");
    this.os = Constants.getOS();
    this.architecture = Constants.getArchitecture();
    this.resourcePath = buildResourcePath();
    this.supported = resourcePath != null && MinicorePlatform.class.getResource(resourcePath) != null;
  }

  public boolean isSupported() {
    return supported;
  }

  public String getLibraryPath() {
    if (!supported) {
      throw new UnsupportedOperationException(
          String.format(
              "Minicore library not available for platform: OS=%s (%s), Arch=%s (%s)",
              os, osName, architecture, osArch));
    }
    return resourcePath;
  }

  private String buildResourcePath() {
    if (os == null || architecture == null) {
      return null;
    }
    
    String osId = getOsIdentifier();
    String archId = getArchIdentifier();
    if (osId == null || archId == null) {
      return null;
    }
    
    String platformId = osId + "-" + archId;
    String libraryFile = getLibraryPrefix() + LIBRARY_BASE_NAME + getLibraryExtension();
    String directoryName = String.format(
        "sf_mini_core_%s_%s_%s",
        platformId, MINICORE_VERSION, MINICORE_GIT_HASH);
    
    return String.format("/minicore/%s/%s", directoryName, libraryFile);
  }

  public String getPlatformIdentifier() {
    String osId = getOsIdentifier();
    String archId = getArchIdentifier();
    if (osId == null || archId == null) {
      return null;
    }
    return osId + "-" + archId;
  }

  private String getOsIdentifier() {
    if (os == null) {
      return null;
    }
    switch (os) {
      case LINUX:
        return "linux";
      case MAC:
        return "macos";
      case WINDOWS:
        return "windows";
      case SOLARIS:
        // AIX is detected as LINUX in Constants, but we need to check the actual OS name
        if (osName != null && osName.toLowerCase().contains("aix")) {
          return "aix";
        }
        return "solaris";
      default:
        return null;
    }
  }

  private String getArchIdentifier() {
    if (architecture == null) {
      return null;
    }
    switch (architecture) {
      case X86_64:
        return "x86_64";
      case AARCH64:
        return "aarch64";
      case PPC64:
        return "ppc64";
      case X86:
        return "x86";
      default:
        return null;
    }
  }

  private String getLibraryPrefix() {
    if (os == null) {
      return "";
    }
    switch (os) {
      case WINDOWS:
        return "";
      case LINUX:
      case MAC:
      case SOLARIS:
      default:
        return "lib";
    }
  }

  private String getLibraryExtension() {
    if (os == null) {
      return "";
    }
    // Check for AIX specifically
    if (osName != null && osName.toLowerCase().contains("aix")) {
      return ".a";
    }
    switch (os) {
      case WINDOWS:
        return ".dll";
      case MAC:
        return ".dylib";
      case LINUX:
      case SOLARIS:
      default:
        return ".so";
    }
  }

  public String getLibraryFileName() {
    return getLibraryPrefix() + LIBRARY_BASE_NAME + getLibraryExtension();
  }

  /**
   * Get the resource path for the minicore library in the JAR.
   *
   * <p>Format: /minicore/sf_mini_core_{platform}_{version}_{hash}/{library_file}
   *
   * <p>Example:
   * /minicore/sf_mini_core_linux-x86_64_0.0.1_SNAPSHOT_47a8f3d456b27d6253cd8020a80dd6fc885169a3/libsf_mini_core.so
   *
   * @return resource path, or null if platform is not supported
   * @deprecated Use {@link #getLibraryPath()} instead, which throws an exception for unsupported platforms
   */
  public String getResourcePath() {
    return resourcePath;
  }

  public OS getOs() {
    return os;
  }

  public String getOsName() {
    return osName;
  }

  public String getOsArch() {
    return osArch;
  }

  @Override
  public String toString() {
    return String.format(
        "MinicorePlatform{os=%s, arch=%s, osName='%s', osArch='%s', supported=%s}",
        os, architecture, osName, osArch, isSupported());
  }
}
