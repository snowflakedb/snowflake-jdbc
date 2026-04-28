package net.snowflake.client.internal.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import net.snowflake.client.internal.jdbc.SnowflakeUtil;
import net.snowflake.client.internal.log.SFLogger;
import net.snowflake.client.internal.log.SFLoggerFactory;

/** Reads the SPCS service-identifier token that the SPCS runtime writes into the container. */
public class SpcsTokenReader {

  private static final SFLogger logger = SFLoggerFactory.getLogger(SpcsTokenReader.class);

  static final String SPCS_RUNNING_INSIDE_ENV_VAR = "SNOWFLAKE_RUNNING_INSIDE_SPCS";
  static final String SPCS_TOKEN_FILE_PATH = "/snowflake/session/spcs_token";

  public String readSpcsToken() {
    if (!isRunningInsideSpcs()) {
      return null;
    }
    try {
      String token = new String(readTokenFileBytes(), StandardCharsets.UTF_8).trim();
      if (token.isEmpty()) {
        logger.warn("SPCS token file at {} is empty", SPCS_TOKEN_FILE_PATH);
        return null;
      }
      return token;
    } catch (Exception ex) {
      logger.warn("Failed to read SPCS token from {}: {}", SPCS_TOKEN_FILE_PATH, ex.getMessage());
      return null;
    }
  }

  // Overridable in tests via Mockito.spy(...). Production reads the real env / filesystem.
  boolean isRunningInsideSpcs() {
    String env = SnowflakeUtil.systemGetEnv(SPCS_RUNNING_INSIDE_ENV_VAR);
    return env != null && !env.isEmpty();
  }

  byte[] readTokenFileBytes() throws IOException {
    return Files.readAllBytes(Paths.get(SPCS_TOKEN_FILE_PATH));
  }
}
