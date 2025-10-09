package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.*;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Test;

// Unit tests for parseParams() function.
public class ConnectionAutoUrlParserTest {
  @Test
  void testValidConnection() throws SnowflakeSQLException {
    String url = "jdbc:snowflake:auto?connection=readonly";
    String value = SFConnectionConfigParser.parseParams(url);
    assertEquals("readonly", value);
  }

  @Test
  void testInvalidPrefix() {
    String url = "jdbc:mysql:auto?connection=readonly";
    SnowflakeSQLException ex =
        assertThrows(SnowflakeSQLException.class, () -> SFConnectionConfigParser.parseParams(url));
    assertTrue(ex.getMessage().contains("must start with jdbc:snowflake:auto"));
  }

  @Test
  void testMissingQueryString() throws SnowflakeSQLException {
    String url = "jdbc:snowflake:auto";
    String value = SFConnectionConfigParser.parseParams(url);
    assertEquals("", value);
  }

  @Test
  void testUnsupportedParameterKey() {
    String url = "jdbc:snowflake:auto?foo=bar";
    SnowflakeSQLException ex =
        assertThrows(SnowflakeSQLException.class, () -> SFConnectionConfigParser.parseParams(url));
    assertTrue(ex.getMessage().contains("Only 'connection' parameter is supported"));
  }

  @Test
  void testMissingValueForConnection() {
    String url = "jdbc:snowflake:auto?connection=";
    SnowflakeSQLException ex =
        assertThrows(SnowflakeSQLException.class, () -> SFConnectionConfigParser.parseParams(url));
    assertTrue(ex.getMessage().contains("must have a value"));
  }
}
