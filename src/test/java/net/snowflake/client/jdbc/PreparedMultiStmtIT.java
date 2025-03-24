package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class PreparedMultiStmtIT extends BaseJDBCWithSharedConnectionIT {
  private static SnowflakeConnectionV1 sfConnectionV1;

  public PreparedMultiStmtIT() {
    this.sfConnectionV1 = (SnowflakeConnectionV1) connection;
  }

  public void setSessionResultFormat(String queryResultFormat) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      stmt.execute("alter session set jdbc_query_result_format = '" + queryResultFormat + "'");
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteUpdateCount(String queryResultFormat) throws Exception {
    setSessionResultFormat(queryResultFormat);
    try (Statement statement = sfConnectionV1.createStatement()) {
      try {
        statement.execute("alter session set MULTI_STATEMENT_COUNT=0");
        statement.execute("create or replace table test_multi_bind(c1 number)");

        try (PreparedStatement preparedStatement =
            sfConnectionV1.prepareStatement(
                "insert into test_multi_bind(c1) values(?); insert into "
                    + "test_multi_bind values (?), (?)")) {

          assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(3));

          preparedStatement.setInt(1, 20);
          preparedStatement.setInt(2, 30);
          preparedStatement.setInt(3, 40);

          // first statement
          int rowCount = preparedStatement.executeUpdate();
          assertThat(rowCount, is(1));
          assertThat(preparedStatement.getResultSet(), is(nullValue()));
          assertThat(preparedStatement.getUpdateCount(), is(1));

          // second statement
          assertThat(preparedStatement.getMoreResults(), is(false));
          assertThat(preparedStatement.getUpdateCount(), is(2));

          try (ResultSet resultSet =
              statement.executeQuery("select c1 from test_multi_bind order by c1 asc")) {
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(20));
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(30));
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(40));
          }
        }
      } finally {
        statement.execute("drop table if exists test_multi_bind");
      }
    }
  }

  /** Less bindings than expected in statement */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteLessBindings(String queryResultFormat) throws Exception {
    setSessionResultFormat(queryResultFormat);
    try (Statement statement = sfConnectionV1.createStatement()) {
      try {
        statement.execute("alter session set MULTI_STATEMENT_COUNT=0");
        statement.execute("create or replace table test_multi_bind(c1 number)");

        try (PreparedStatement preparedStatement =
            sfConnectionV1.prepareStatement(
                "insert into test_multi_bind(c1) values(?); insert into "
                    + "test_multi_bind values (?), (?)")) {

          assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(3));

          preparedStatement.setInt(1, 20);
          preparedStatement.setInt(2, 30);

          // first statement
          SQLException e =
              assertThrows(SQLException.class, () -> preparedStatement.executeUpdate());
          assertThat(e.getErrorCode(), is(100132));
        }
      } finally {
        statement.execute("drop table if exists test_multi_bind");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteMoreBindings(String queryResultFormat) throws Exception {
    setSessionResultFormat(queryResultFormat);
    try (Statement statement = sfConnectionV1.createStatement()) {
      try {
        statement.execute("alter session set MULTI_STATEMENT_COUNT=0");
        statement.execute("create or replace table test_multi_bind(c1 number)");

        try (PreparedStatement preparedStatement =
            sfConnectionV1.prepareStatement(
                "insert into test_multi_bind(c1) values(?); insert into "
                    + "test_multi_bind values (?), (?)")) {

          assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(3));

          preparedStatement.setInt(1, 20);
          preparedStatement.setInt(2, 30);
          preparedStatement.setInt(3, 40);
          // 4th binding should be ignored
          preparedStatement.setInt(4, 50);

          // first statement
          int rowCount = preparedStatement.executeUpdate();
          assertThat(rowCount, is(1));
          assertThat(preparedStatement.getResultSet(), is(nullValue()));
          assertThat(preparedStatement.getUpdateCount(), is(1));

          // second statement
          assertThat(preparedStatement.getMoreResults(), is(false));
          assertThat(preparedStatement.getUpdateCount(), is(2));

          try (ResultSet resultSet =
              statement.executeQuery("select c1 from test_multi_bind order by c1 asc")) {
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(20));
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(30));
            assertTrue(resultSet.next());
            assertThat(resultSet.getInt(1), is(40));
          }
        }
      } finally {
        statement.execute("drop table if exists test_multi_bind");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteQueryBindings(String queryResultFormat) throws Exception {
    setSessionResultFormat(queryResultFormat);
    try (Statement statement = sfConnectionV1.createStatement()) {
      statement.execute("alter session set MULTI_STATEMENT_COUNT=0");

      try (PreparedStatement preparedStatement =
          sfConnectionV1.prepareStatement("select ?; select ?, ?; select ?, ?, ?")) {

        assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(6));

        preparedStatement.setInt(1, 10);
        preparedStatement.setInt(2, 20);
        preparedStatement.setInt(3, 30);
        preparedStatement.setInt(4, 40);
        preparedStatement.setInt(5, 50);
        preparedStatement.setInt(6, 60);

        // first statement
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.getInt(1), is(10));
        }
        // second statement
        assertThat(preparedStatement.getMoreResults(), is(true));
        try (ResultSet resultSet = preparedStatement.getResultSet()) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt(1), is(20));
          assertThat(resultSet.getInt(2), is(30));
        }

        // third statement
        assertThat(preparedStatement.getMoreResults(), is(true));
        try (ResultSet resultSet = preparedStatement.getResultSet()) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt(1), is(40));
          assertThat(resultSet.getInt(2), is(50));
          assertThat(resultSet.getInt(3), is(60));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteQueryNoBindings(String queryResultFormat) throws Exception {
    setSessionResultFormat(queryResultFormat);
    try (Statement statement = sfConnectionV1.createStatement()) {
      statement.execute("alter session set MULTI_STATEMENT_COUNT=0");

      try (PreparedStatement preparedStatement =
          sfConnectionV1.prepareStatement("select 10; select 20, 30; select 40, 50, 60")) {

        assertThat(preparedStatement.getParameterMetaData().getParameterCount(), is(0));

        // first statement
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.getInt(1), is(10));
        }

        // second statement
        assertThat(preparedStatement.getMoreResults(), is(true));
        try (ResultSet resultSet = preparedStatement.getResultSet()) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt(1), is(20));
          assertThat(resultSet.getInt(2), is(30));
        }

        // third statement
        assertThat(preparedStatement.getMoreResults(), is(true));
        try (ResultSet resultSet = preparedStatement.getResultSet()) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt(1), is(40));
          assertThat(resultSet.getInt(2), is(50));
          assertThat(resultSet.getInt(3), is(60));
        }
      }
    }
  }
}
