package net.snowflake.client.log;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.logging.Level;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JDK14LoggerTest {

  @Test
  @Disabled
  public void testLegacyLoggerInit() throws IOException {
    System.setProperty("snowflake.jdbc.log.size", "100000");
    System.setProperty("snowflake.jdbc.log.count", "3");
    System.setProperty("net.snowflake.jdbc.loggerImpl", "net.snowflake.client.log.JDK14Logger");

    JDK14Logger logger = new JDK14Logger(JDK14LoggerTest.class.getName());
    assertFalse(logger.isDebugEnabled());
    assertTrue(logger.isInfoEnabled());

    String level = "all";
    Level tracingLevel = Level.parse(level.toUpperCase());
    String logOutputPath =
        Paths.get(systemGetProperty("java.io.tmpdir"), "snowflake_jdbc.log").toString();
    JDK14Logger.instantiateLogger(tracingLevel, logOutputPath);
    assertTrue(logger.isDebugEnabled());
  }
}
