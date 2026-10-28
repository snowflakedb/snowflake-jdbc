package net.snowflake.client.internal.core;

import java.util.Map;

/** Gives tests outside this package access to the session-parameter path used at login. */
public final class SessionParameterTestUtil {
  private SessionParameterTestUtil() {}

  /** Applies {@code parameters} to {@code session} the same way login and query responses do. */
  public static void applyParameterBlock(Map<String, Object> parameters, SFBaseSession session) {
    SessionUtil.updateSfDriverParamValues(parameters, session);
  }
}
