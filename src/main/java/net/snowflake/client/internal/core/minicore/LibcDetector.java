package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Platform;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class distinguishes between glibc (GNU C Library) and musl libc by attempting to call
 * the glibc-specific function gnu_get_libc_version().
 */
@SnowflakeJdbcInternalApi
public class LibcDetector {

  private static final SFLogger logger = SFLoggerFactory.getLogger(LibcDetector.class);

  public enum LibcVariant {
    GLIBC("glibc"),
    MUSL("musl"),
    UNSUPPORTED("unsupported");

    private final String identifier;

    LibcVariant(String identifier) {
      this.identifier = identifier;
    }

    public String getIdentifier() {
      return identifier;
    }
  }

  public static LibcVariant detectLibcVariant() {
    if (!Platform.isLinux()) {
      return LibcVariant.UNSUPPORTED;
    }

    if (LibC.INSTANCE == null) {
      logger.warn("Failed to load C library, defaulting to musl");
      return LibcVariant.MUSL;
    }

    try {
      String version = LibC.INSTANCE.gnu_get_libc_version();
      logger.trace("Successfully called gnu_get_libc_version(), version: {}", version);
      return LibcVariant.GLIBC;

    } catch (UnsatisfiedLinkError e) {
      logger.trace("gnu_get_libc_version() not found, using musl: {}", e.getMessage());
      return LibcVariant.MUSL;

    } catch (Throwable t) {
      logger.warn("Error calling gnu_get_libc_version(), defaulting to musl: {}", t.getMessage());
      return LibcVariant.MUSL;
    }
  }
}

