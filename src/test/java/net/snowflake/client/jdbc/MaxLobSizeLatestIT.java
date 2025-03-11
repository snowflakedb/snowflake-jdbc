package net.snowflake.client.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(TestTags.STATEMENT)
public class MaxLobSizeLatestIT extends BaseJDBCTest {

  /**
   * Available since 3.17.0
   *
   * @throws SQLException
   */
  @Test
  @DontRunOnGithubActions
  public void testIncreasedMaxLobSize() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY='ENABLED'");
      stmt.execute("alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=false");
      SnowflakeSQLException e =
          assertThrows(
              SnowflakeSQLException.class,
              () -> stmt.execute("select randstr(20000000, random()) as large_str"));
      assertThat(e.getMessage(), CoreMatchers.containsString("exceeds supported length"));

      stmt.execute("alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=true");
      try (ResultSet resultSet =
          stmt.executeQuery("select randstr(20000000, random()) as large_str")) {
        assertTrue(resultSet.next());
        assertThat(resultSet.getString(1), is(not(emptyOrNullString())));
      } finally {
        stmt.execute("alter session unset ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT");
        stmt.execute("alter session unset FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY");
      }
    }
  }
}
