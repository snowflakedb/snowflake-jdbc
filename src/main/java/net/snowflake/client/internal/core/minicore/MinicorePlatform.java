package net.snowflake.client.internal.core.minicore;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

import net.snowflake.client.core.Constants;
import net.snowflake.client.core.Constants.Architecture;
import net.snowflake.client.core.Constants.OS;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

@SnowflakeJdbcInternalApi
public class MinicorePlatform {
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
    this.supported =
        resourcePath != null && MinicorePlatform.class.getResource(resourcePath) != null;
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
    String fileName = getLibraryFileName();
    if (fileName == null) {
      return null;
    }
    // Flat structure: /minicore/{filename}
    return "/minicore/" + fileName;
  }

  public String getPlatformIdentifier() {
    String osId = getOsIdentifier();
    if (osId == null || architecture == Architecture.UNKNOWN) {
      return null;
    }

    String archId = architecture.getIdentifier();

    // For Linux, add libc variant: linux-x86_64-glibc or linux-aarch64-musl
    LibcDetector.LibcVariant libcVariant = LibcDetector.detectLibcVariant();
    if (libcVariant != LibcDetector.LibcVariant.UNSUPPORTED) {
      return osId + "-" + archId + "-" + libcVariant.getIdentifier();
    }

    // For other OSes (UNSUPPORTED), just os-arch
    return osId + "-" + archId;
  }

  /** Get OS identifier (used in filenames). Returns: macos, linux, windows, aix. */
  private String getOsIdentifier() {
    if (os == null) {
      return null;
    }
    switch (os) {
      case LINUX:
        if (Constants.isAix()) {
          return "aix";
        }
        return "linux";
      case MAC:
        return "macos";
      case WINDOWS:
        return "windows";
      default:
        return null;
    }
  }

  private String getLibraryExtension() {
    if (os == null) {
      return "";
    }
    if (Constants.isAix()) {
      return ".so";
    }
    switch (os) {
      case WINDOWS:
        return ".dll";
      case MAC:
        return ".dylib";
      default: // Linux included.
        return ".so";
    }
  }

  /**
   * Get the library filename with platform encoding.
   *
   * <p>Format: {base_name}_{os}_{arch}[_{libc}]{extension}
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>Linux x86_64 glibc: {@code libsf_mini_core_linux_x86_64_glibc.so}
   *   <li>Linux aarch64 musl: {@code libsf_mini_core_linux_aarch64_musl.so}
   *   <li>macOS x86_64: {@code libsf_mini_core_macos_x86_64.dylib}
   *   <li>macOS aarch64: {@code libsf_mini_core_macos_aarch64.dylib}
   *   <li>Windows x86_64: {@code libsf_mini_core_windows_x86_64.dll}
   *   <li>AIX ppc64: {@code libsf_mini_core_aix_ppc64.so}
   * </ul>
   */
  public String getLibraryFileName() {
    String osId = getOsIdentifier();
    if (osId == null || architecture == Architecture.UNKNOWN) {
      return null;
    }

    String archId = architecture.getIdentifier();

    StringBuilder fileName = new StringBuilder();
    fileName.append(Minicore.LIBRARY_BASE_NAME);
    fileName.append("_").append(osId);
    fileName.append("_").append(archId);

    // For Linux, add libc variant
    LibcDetector.LibcVariant libcVariant = LibcDetector.detectLibcVariant();
    if (libcVariant != LibcDetector.LibcVariant.UNSUPPORTED) {
      fileName.append("_").append(libcVariant.getIdentifier());
    }

    fileName.append(getLibraryExtension());
    return fileName.toString();
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
