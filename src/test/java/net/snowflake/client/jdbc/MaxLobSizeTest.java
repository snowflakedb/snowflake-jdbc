package net.snowflake.client.jdbc;

import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.text.IsEmptyString;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.MatcherAssert.*;

public class MaxLobSizeTest extends BaseJDBCTest {

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
            ResultSet resultSet = stmt.executeQuery("select randstr(20000000, random()) as large_str");
            resultSet.next();
            assertThat(resultSet.getString(1), IsEmptyString.emptyOrNullString());

        }
    }
}
