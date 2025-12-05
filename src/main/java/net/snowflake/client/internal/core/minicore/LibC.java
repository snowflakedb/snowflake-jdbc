package net.snowflake.client.internal.core.minicore;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/** JNA interface for the C standard library (libc). */
@SnowflakeJdbcInternalApi
interface LibC extends Library {

  LibC INSTANCE = loadLibC();

  String gnu_get_libc_version();

  static LibC loadLibC() {
    try {
      return Native.load(Platform.C_LIBRARY_NAME, LibC.class);
    } catch (Throwable t) {
      return null;
    }
  }
}
