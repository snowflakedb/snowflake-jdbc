package net.snowflake.client.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class SnowparkTest {

  @Test
  void test() throws SQLException {
    Properties props = new Properties();
    props.put("user", "admin");
    props.put("password", "test");
    props.put("warehouse", "regress");
    props.put("db", "test");
    props.put("schema", "public");
    props.put("ssl", "off");

    String url = "jdbc:snowflake://snowflake.reg.local:53200";

    try (SnowflakeConnectionV1 conn = new SnowflakeConnectionV1(url, props)) {
      ResultSet result =
          ((SnowflakeStatementV1) conn.createStatement())
              .executeDataframeAst("SGVsbG8sIFdvcmxkIQ==");
      result.next();
      System.out.println(result.getString(1));
    }
  }
}
