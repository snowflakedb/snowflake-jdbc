package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Platform;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * This class distinguishes between glibc (GNU C Library) and musl libc by attempting to call the
 * glibc-specific function gnu_get_libc_version().
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
      logger.trace("Failed to load C library, cannot detect libc variant");
      return LibcVariant.UNSUPPORTED;
    }

    try {
      String version = LibC.INSTANCE.gnu_get_libc_version();
      logger.trace("Successfully called gnu_get_libc_version(), version: {}", version);
      return LibcVariant.GLIBC;

    } catch (UnsatisfiedLinkError e) {
      // gnu_get_libc_version is glibc-specific, so if it's not found, this is musl
      logger.trace("gnu_get_libc_version() not found, detected musl: {}", e.getMessage());
      return LibcVariant.MUSL;

    } catch (Throwable t) {
      logger.trace("Error calling gnu_get_libc_version(): {}", t.getMessage());
      return LibcVariant.UNSUPPORTED;
    }
  }
}
