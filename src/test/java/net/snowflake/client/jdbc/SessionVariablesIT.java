/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Created by tcruanes on 4/24/17. */
@Category(TestCategoryOthers.class)
public final class SessionVariablesIT extends AbstractDriverIT {
  private static Logger logger = Logger.getLogger(BaseJDBCTest.class.getName());

  private static void sql(final Connection connection, String sqlText) throws SQLException {
    // Create a warehouse for the test
    Statement stmt = connection.createStatement();
    stmt.setMaxRows(1);
    boolean hasResultSet = stmt.execute(sqlText);
    if (hasResultSet) {
      assertTrue(stmt.getResultSet().next());
    }
    stmt.close();
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSessionVariables() throws SQLException, IOException {
    // build connection properties
    Properties properties = new Properties();
    properties.put("var1", "some example");
    properties.put("var2", "1");
    properties.put("$var3", "some example");
    properties.put("$var4", "1");

    // Open a connection under the snowflake account and enable variable support
    Connection con = getSnowflakeAdminConnection(properties);
    sql(
        con,
        "alter system set"
            + " enable_assignment_scalar=true,"
            + " enable_assignment_statement=true");
    con.close();

    // Create a new connection passing along some variables.
    properties.put("$var1", "some example");
    properties.put("$var2", 10L);
    properties.put("var1", "ignored");
    properties.put("var2", "ignored");
    con = getConnection(properties);

    // Check that the variables are set in the session
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery("show variables");

    // expecting at least two rows... up to 4 variables
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());

    statement.close();
    con.close();

    con = getSnowflakeAdminConnection(properties);
    sql(con, "alter system unset enable_assignment_scalar, enable_assignment_statement");
    con.close();
  }
}
