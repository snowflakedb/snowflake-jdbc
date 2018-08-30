/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This is just a simple test class for compatibility check. Make sure that
 * customer can load com.snowflake.client.jdbc.SnowflakeDriver class
 * Created by hyu on 10/10/16.
 */
public class ComSnowflakeDriverIT extends AbstractDriverIT
{
  @Test
  public void testSimpleConnection() throws SQLException
  {
    Connection connection = getConnection();
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("show parameters");
    assert (resultSet.next());
    connection.close();
  }
}
