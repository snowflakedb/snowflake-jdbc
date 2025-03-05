package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.RESULT_SET)
public class GCPLargeResult extends BaseJDBCTest {

  Connection init(String queryResultFormat) throws SQLException {
    Connection conn = BaseJDBCTest.getConnection("gcpaccount");
    System.out.println("Connected");
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
    return conn;
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testLargeResultSetGCP(String queryResultFormat) throws Throwable {
    try (Connection con = init(queryResultFormat);
        PreparedStatement stmt =
            con.prepareStatement(
                "select seq8(), randstr(1000, random()) from table(generator(rowcount=>1000))")) {
      stmt.setMaxRows(999);
      try (ResultSet rset = stmt.executeQuery()) {
        int cnt = 0;
        while (rset.next()) {
          ++cnt;
        }
        assertEquals(cnt, 999);
      }
    }
  }
}
