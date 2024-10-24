package com.snowflake.client.jdbc;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryConnection;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(TestCategoryConnection.class)
public class SnowflakeDriverIT extends AbstractDriverIT {

  @Test
  public void testConnection() throws SQLException {
    Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, true);
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }
}
