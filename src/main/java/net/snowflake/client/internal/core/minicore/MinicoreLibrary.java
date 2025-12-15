package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Library;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** JNA interface for the Snowflake minicore native library. */
@SnowflakeJdbcInternalApi
public interface MinicoreLibrary extends Library {

  /**
   * Get the full version string from the minicore library. This method maps to the C function:
   * const char* sf_core_full_version();
   */
  String sf_core_full_version();
}
