package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.Ignore;
import org.junit.Test;

public class TestClass extends BaseJDBCTest {

  @Ignore
  @Test
  public void Test() throws SQLException {
    String account = "dbsec_internal_mfa_changes_testing_15.preprod12.us-west-2.aws";
    Properties props = new Properties();
    props.put("useProxy", true);
    props.put("proxyHost", "localhost");
    props.put("proxyPort", "8080");
    props.put("user", "");
    props.put("password", "");
    Connection conn =
        DriverManager.getConnection("jdbc:snowflake://" + account + "snowflake.com", props);
    conn.getClientInfo();
    Statement stmt = conn.createStatement();
    stmt.executeQuery("select 1");
    ResultSet rs = stmt.getResultSet();
    while (rs.next()) {
      System.out.println(rs.getInt(1));
    }
  }
}
