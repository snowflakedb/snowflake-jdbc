package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class DataframeAstIT {

  @Test
  public void simpleEndToEndTest() throws SQLException {
    Properties props = new Properties();
    String url = "jdbc:sqlite:test.db";
    try (SnowflakeConnectionV1 conn = new SnowflakeConnectionV1(url, props)) {
      ResultSet result =
          ((SnowflakeStatementV1) conn.createStatement()).executeDataframeAst("dummy");
    }
  }
}
