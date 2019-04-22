package com.snowflake.client.jdbc;

import net.snowflake.client.AbstractDriverIT;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertTrue;

public class SnowflakeDriverIT extends AbstractDriverIT
{

  @Test
  public void testConnection() throws SQLException
  {
    Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, true);
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }
}


