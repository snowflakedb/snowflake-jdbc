package net.snowflake.client.jdbc.cloud.storage;

import static net.snowflake.client.AbstractDriverIT.getConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Test;

public class TestPut {
  @Test
  public void putTest() throws SQLException {
    try (Connection con = getConnection()) {
      Statement stmt = con.createStatement();
    }
  }
}
