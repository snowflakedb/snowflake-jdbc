package net.snowflake.client.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.text.IsEmptyString.emptyOrNullString;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

public class MaxLobSizeLatestIT extends BaseJDBCTest {

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testMaxLobSize() throws SQLException {
    try (Connection con = BaseJDBCTest.getConnection();
        Statement stmt = con.createStatement()) {
      stmt.execute("alter session set FEATURE_INCREASED_MAX_LOB_SIZE_IN_MEMORY='ENABLED'");
      stmt.execute("alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=false");
      try {
        stmt.execute("select randstr(20000000, random()) as large_str");
      } catch (SnowflakeSQLException e) {
        assertThat(e.getMessage(), CoreMatchers.containsString("exceeds supported length"));
      }

      stmt.execute("alter session set ENABLE_LARGE_VARCHAR_AND_BINARY_IN_RESULT=true");
      try (ResultSet resultSet =
          stmt.executeQuery("select randstr(20000000, random()) as large_str")) {
        Assert.assertTrue(resultSet.next());
        assertThat(resultSet.getString(1), is(not(emptyOrNullString())));
      }
    }
  }
}
