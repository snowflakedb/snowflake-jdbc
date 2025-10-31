package net.snowflake.client.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

// Unit tests for getConnectionNameFromUrl() function.
public class ConnectionAutoUrlParserTest {
  @Test
  void testValidConnection() {
    String url = "jdbc:snowflake:auto?connectionName=readonly";
    String value = SFConnectionConfigParser.getConnectionNameFromUrl(url);
    assertEquals("readonly", value);
  }

  @Test
  void testNoParameters() {
    String url = "jdbc:snowflake:auto";
    String value = SFConnectionConfigParser.getConnectionNameFromUrl(url);
    assertEquals("", value);
  }

  @Test
  void testUTFCharParameterKey() {
    String url =
        "jdbc:snowflake://account.region.snowflakecomputing.com/?"
            + "user=vikram&password=secret&connectionName=myConfig&note=%E2%9C%93";

    assertEquals("myConfig", SFConnectionConfigParser.getConnectionNameFromUrl(url));
  }

  @Test
  void testMissingValueForConnection() {
    String url = "jdbc:snowflake:auto?connectionName=";
    assertEquals("", SFConnectionConfigParser.getConnectionNameFromUrl(url));
  }
}
