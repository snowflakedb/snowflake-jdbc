package com.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.CONNECTION)
public class SnowflakeDriverIT extends AbstractDriverIT {

  @Test
  public void testConnection() throws SQLException {
    Connection con = getConnection(DONT_INJECT_SOCKET_TIMEOUT, null, false, true);
    con.close();
    assertTrue(con.isClosed());
    con.close(); // ensure no exception
  }
}
