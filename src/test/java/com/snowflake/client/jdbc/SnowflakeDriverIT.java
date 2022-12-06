package com.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import org.junit.Test;

public class SnowflakeDriverIT extends AbstractDriverIT {

  @Test
  public void testConnection() throws SQLException {
    // Properties properties = new Properties();
    // properties.put("networkTimeout", 2000);
    // Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, properties, false, true);
    Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, true);

    // con.setNetworkTimeout(null, 200);

    Statement stmt = con.createStatement();
    // ResultSet rs = stmt.executeQuery("select * from harsh_test_schema.harsh_test_table where
    // ycsb_key = 98333");
    ResultSet rs = stmt.executeQuery("select 1");
    int rowIdx = 0;
    int count = 0;
    while (rs.next() && count <= 10) {
      rowIdx++;
      count++;
      System.out.println("Current count is " + count);
    }
    stmt.close();
    con.close();
    // assertTrue(con.isClosed());
    // con.close(); // ensure no exception
  }
}
