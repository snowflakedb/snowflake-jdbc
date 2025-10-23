package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.snowflake.client.jdbc.SnowflakeSQLException;
import org.junit.jupiter.api.Test;

// Unit tests for parseParams() function.
public class ConnectionAutoUrlParserTest {
  @Test
  void testValidConnection() throws SnowflakeSQLException {
    String url = "jdbc:snowflake:auto?connection=readonly";
    String value = SFConnectionConfigParser.getConnectionNameFromUrl(url);
    assertEquals("readonly", value);
  }

  @Test
  void testMissingQueryString() throws SnowflakeSQLException {
    String url = "jdbc:snowflake:auto";
    String value = SFConnectionConfigParser.getConnectionNameFromUrl(url);
    assertEquals("", value);
  }

  @Test
  void testUnsupportedParameterKey() {
    String url = "jdbc:snowflake:auto?foo=bar";
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SFConnectionConfigParser.getConnectionNameFromUrl(url));
    assertTrue(ex.getMessage().contains("Only 'connection' parameter is supported"));
  }

  @Test
  void testMissingValueForConnection() {
    String url = "jdbc:snowflake:auto?connection=";
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () -> SFConnectionConfigParser.getConnectionNameFromUrl(url));
    assertTrue(ex.getMessage().contains("must have a value"));
  }
}
