package net.snowflake.client.internal.util;

/**
 * Immutable libc detection result returned by {@link LibcDetails#load()}.
 *
 * <p>Both {@code family} and {@code version} may be {@code null} - e.g. on non-Linux platforms
 * (both null), or on musl when no version source could be parsed (family set, version null).
 */
public final class LibcInfo {

  private final String family;
  private final String version;

  public LibcInfo(String family, String version) {
    this.family = family;
    this.version = version;
  }

  public String getFamily() {
    return family;
  }

  public String getVersion() {
    return version;
  }
}
