package net.snowflake.client.jdbc;

import static net.snowflake.client.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** This test could be run only on environment where file connection.toml is configured */
@Disabled
public class AutoConnectionConfigurationLatestIT extends BaseJDBCTest {

  @AfterEach
  public void cleanUp() {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
  }

  @Test
  public void testThrowExceptionIfConfigurationDoesNotExist() throws SQLException {
    SnowflakeSQLException ex =
        assertThrows(
            SnowflakeSQLException.class,
            () ->
                DriverManager.getConnection(
                    SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX + "?connection=notConfigured",
                    null));
    assertTrue(
        ex.getMessage()
            .contains(
                "Unavailable connection configuration parameters expected for auto configuration"));
  }

  @Test
  public void testConnectionParameterConfiguredInURL() throws SQLException {
    try (Connection con =
            DriverManager.getConnection(
                SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX + "?connection= readonly", null);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      assertTrue(resultSet.next());
    }
  }

  @Test
  public void testSimpleConnectionUsingFileConfigurationToken() throws SQLException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, "SystemVariableConfig");
    try (Connection con =
            DriverManager.getConnection(SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX, null);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      assertTrue(resultSet.next());
    }
  }

  @Test
  public void testDefaultConnectionFromFile() throws SQLException {
    try (Connection con =
            DriverManager.getConnection(SnowflakeDriver.AUTO_CONNECTION_STRING_PREFIX, null);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      assertTrue(resultSet.next());
    }
  }
}
