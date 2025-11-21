package net.snowflake.client.internal.jdbc;

import static net.snowflake.client.internal.config.SFConnectionConfigParser.SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.api.driver.SnowflakeDriver;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.driver.AutoConfigurationHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/** This test could be run only on environment where file connection.toml is configured */
@Disabled
public class FileConnectionConfigurationLatestIT {

  @AfterEach
  public void cleanUp() {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY);
  }

  @Test
  public void testThrowExceptionIfConfigurationDoesNotExist() {
    SnowflakeUtil.systemSetEnv("SNOWFLAKE_DEFAULT_CONNECTION_NAME", "non-existent");
    assertThrows(SnowflakeSQLException.class, () -> SnowflakeDriver.INSTANCE.connect());
  }

  @Test
  public void testSimpleConnectionUsingFileConfigurationToken() throws SQLException {
    verifyConnetionToSnowflake("aws-oauth");
  }

  @Test
  public void testSimpleConnectionUsingFileConfigurationTokenFromFile() throws SQLException {
    verifyConnetionToSnowflake("aws-oauth-file");
  }

  private static void verifyConnetionToSnowflake(String connectionName) throws SQLException {
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_KEY, connectionName);
    try (Connection con =
            DriverManager.getConnection(AutoConfigurationHelper.AUTO_CONNECTION_PREFIX, null);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      assertTrue(resultSet.next());
    }
  }
}
