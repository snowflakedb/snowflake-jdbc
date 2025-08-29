package net.snowflake.client.core.crl;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import net.snowflake.client.core.Constants;

class CRLValidationConfig {

  enum CertRevocationCheckMode {
    DISABLED,
    ENABLED,
    ADVISORY
  }

  private final CertRevocationCheckMode certRevocationCheckMode;
  private final boolean allowCertificatesWithoutCrlUrl;
  private final int connectionTimeoutMs;
  private final int readTimeoutMs;
  private final boolean inMemoryCacheEnabled;
  private final boolean onDiskCacheEnabled;
  private final Duration cacheValidityTime;
  private final Path onDiskCacheDir;
  private final Duration onDiskCacheRemovalDelay;

  private CRLValidationConfig(Builder builder) {
    this.certRevocationCheckMode = builder.certRevocationCheckMode;
    this.allowCertificatesWithoutCrlUrl = builder.allowCertificatesWithoutCrlUrl;
    this.connectionTimeoutMs = builder.connectionTimeoutMs;
    this.readTimeoutMs = builder.readTimeoutMs;
    this.inMemoryCacheEnabled = builder.inMemoryCacheEnabled;
    this.onDiskCacheEnabled = builder.onDiskCacheEnabled;
    this.cacheValidityTime = builder.cacheValidityTime;
    this.onDiskCacheDir = builder.onDiskCacheDir;
    this.onDiskCacheRemovalDelay = builder.onDiskCacheRemovalDelay;
  }

  CertRevocationCheckMode getCertRevocationCheckMode() {
    return certRevocationCheckMode;
  }

  boolean isAllowCertificatesWithoutCrlUrl() {
    return allowCertificatesWithoutCrlUrl;
  }

  int getConnectionTimeoutMs() {
    return connectionTimeoutMs;
  }

  int getReadTimeoutMs() {
    return readTimeoutMs;
  }

  boolean isInMemoryCacheEnabled() {
    return inMemoryCacheEnabled;
  }

  boolean isOnDiskCacheEnabled() {
    return onDiskCacheEnabled;
  }

  Duration getCacheValidityTime() {
    return cacheValidityTime;
  }

  Path getOnDiskCacheDir() {
    return onDiskCacheDir;
  }

  Duration getOnDiskCacheRemovalDelay() {
    return onDiskCacheRemovalDelay;
  }

  static Builder builder() {
    return new Builder();
  }

  static class Builder {
    private CertRevocationCheckMode certRevocationCheckMode = CertRevocationCheckMode.DISABLED;
    private boolean allowCertificatesWithoutCrlUrl = false;
    private int connectionTimeoutMs = 30000; // 30 seconds
    private int readTimeoutMs = 30000; // 30 seconds
    private boolean inMemoryCacheEnabled = true; // Enabled by default
    private boolean onDiskCacheEnabled = true; // Enabled by default
    private Duration cacheValidityTime = Duration.ofDays(1); // Default: 24 hours
    private Path onDiskCacheDir = getDefaultCacheDir();
    private Duration onDiskCacheRemovalDelay = Duration.ofDays(7); // Default: 7 days

    Builder certRevocationCheckMode(CertRevocationCheckMode mode) {
      this.certRevocationCheckMode = mode;
      return this;
    }

    Builder allowCertificatesWithoutCrlUrl(boolean allow) {
      this.allowCertificatesWithoutCrlUrl = allow;
      return this;
    }

    Builder connectionTimeoutMs(int timeoutMs) {
      this.connectionTimeoutMs = timeoutMs;
      return this;
    }

    Builder readTimeoutMs(int timeoutMs) {
      this.readTimeoutMs = timeoutMs;
      return this;
    }

    Builder inMemoryCacheEnabled(boolean enabled) {
      this.inMemoryCacheEnabled = enabled;
      return this;
    }

    Builder onDiskCacheEnabled(boolean enabled) {
      this.onDiskCacheEnabled = enabled;
      return this;
    }

    Builder cacheValidityTime(Duration duration) {
      this.cacheValidityTime = duration;
      return this;
    }

    Builder onDiskCacheDir(Path cacheDir) {
      this.onDiskCacheDir = cacheDir;
      return this;
    }

    Builder onDiskCacheRemovalDelay(Duration duration) {
      this.onDiskCacheRemovalDelay = duration;
      return this;
    }

    CRLValidationConfig build() {
      return new CRLValidationConfig(this);
    }

    private static Path getDefaultCacheDir() {
      String userHome = System.getProperty("user.home");

      if (Constants.getOS() == Constants.OS.WINDOWS) {
        // Windows: %USERPROFILE\AppData\Local\Snowflake\Caches\crls
        return Paths.get(userHome, "AppData", "Local", "Snowflake", "Caches", "crls");
      } else if (Constants.getOS() == Constants.OS.MAC) {
        // MacOS: $HOME/Library/Caches/Snowflake/crls
        return Paths.get(userHome, "Library", "Caches", "Snowflake", "crls");
      } else {
        // Linux/Unix: $HOME/.cache/snowflake/crls
        return Paths.get(userHome, ".cache", "snowflake", "crls");
      }
    }
  }
}
