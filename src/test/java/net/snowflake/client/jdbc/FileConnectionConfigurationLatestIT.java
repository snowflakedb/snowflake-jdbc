/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** This test could be run only on environment where file connection.toml is configured */
@Ignore
public class FileConnectionConfigurationLatestIT {

  public static final String SNOWFLAKE_DEFAULT_CONNECTION_NAME_ENV_VAR =
      "SNOWFLAKE_DEFAULT_CONNECTION_NAME";

  @After
  public void cleanUp() {
    SnowflakeUtil.systemUnsetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_ENV_VAR);
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
    SnowflakeUtil.systemSetEnv(SNOWFLAKE_DEFAULT_CONNECTION_NAME_ENV_VAR, connectionName);
    try (Connection con = DriverManager.getConnection("spcs", null);
        Statement statement = con.createStatement();
        ResultSet resultSet = statement.executeQuery("show parameters")) {
      Assert.assertTrue(resultSet.next());
    }
  }
}
