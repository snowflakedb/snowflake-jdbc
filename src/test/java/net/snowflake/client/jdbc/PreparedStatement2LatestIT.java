package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.PreparedStatement1IT.bindOneParamSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

/**
 * PreparedStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still are not applicable. If it is applicable, move tests to PreparedStatement2IT so
 * that both the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.STATEMENT)
public class PreparedStatement2LatestIT extends PreparedStatement0IT {

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareUDTF(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table employee(id number, address text)");
        statement.execute(
            "create or replace function employee_detail(sid number, addr text)\n"
                + " returns table(id number, address text)\n"
                + "LANGUAGE SQL\n"
                + "as\n"
                + "$$\n"
                + "select *\n"
                + "from employee\n"
                + "where  id=sid\n"
                + "$$;");

        // should resolve successfully
        try (PreparedStatement prepStatement =
            connection.prepareStatement("select * from table(employee_detail(?, ?))")) {
          prepStatement.setInt(1, 1);
          prepStatement.setString(2, "abc");
          prepStatement.execute();
        }

        // should resolve successfully
        try (PreparedStatement prepStatement =
            connection.prepareStatement("select * from table(employee_detail(?, 'abc'))")) {

          prepStatement.setInt(1, 1);
          prepStatement.execute();
        }

        try (PreparedStatement prepStatement =
            connection.prepareStatement("select * from table(employee_detail(?, 123))"); ) {
          // second argument is invalid
          prepStatement.setInt(1, 1);
          SQLException e = assertThrows(SQLException.class, prepStatement::execute);
          // failed because argument type did not match
          assertThat(e.getErrorCode(), is(1044));
        }

        // create a udf with same name but different arguments and return type
        statement.execute(
            "create or replace function employee_detail(name text , addr text)\n"
                + " returns table(id number)\n"
                + "LANGUAGE SQL\n"
                + "as\n"
                + "$$\n"
                + "select id\n"
                + "from employee\n"
                + "$$;");

        try (PreparedStatement prepStatement =
            connection.prepareStatement("select * from table(employee_detail(?, 'abc'))")) {
          prepStatement.setInt(1, 1);
          prepStatement.execute();
        }
      } finally {
        statement.execute("drop function if exists employee_detail(number, text)");
        statement.execute("drop function if exists employee_detail(text, text)");
      }
    }
  }

  /**
   * SNOW-88426: skip bind parameter index check if prepare fails and defer the error checks to
   * execute
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSelectWithBinding(String queryResultFormat) throws Throwable {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table TESTNULL(created_time timestamp_ntz, mid int)");
        // skip bind parameter index check if prepare fails and defer the error checks to execute
        try (PreparedStatement ps =
            connection.prepareStatement(
                "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?, 3) and MID = ?")) {
          ps.setObject(1, 0);
          ps.setObject(2, null);
          try (ResultSet rs = ps.executeQuery()) {
            assertFalse(rs.next());
          }
        }

        // describe is success and do the index range check
        try (PreparedStatement ps =
            connection.prepareStatement(
                "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?::NUMBER, 3) and MID = ?")) {
          ps.setObject(1, 0);
          ps.setObject(2, null);

          try (ResultSet rs = ps.executeQuery()) {
            assertFalse(rs.next());
          }
        }
      } finally {
        statement.execute("drop table if exists TESTNULL");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testLimitBind(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      String stmtStr = "select seq4() from table(generator(rowcount=>100)) limit ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr)) {
        prepStatement.setInt(1, 10);
        prepStatement.executeQuery(); // ensure no error is raised.
      }
    }
  }

  /** SNOW-31746 */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testConstOptLimitBind(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      String stmtStr = "select 1 limit ? offset ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr)) {
        prepStatement.setInt(1, 10);
        prepStatement.setInt(2, 0);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getInt(1), is(1));
          assertThat(resultSet.next(), is(false));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testTableFuncBindInput(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(tableFuncSQL)) {
        prepStatement.setInt(1, 2);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testExecuteLargeBatch(String queryResultFormat) throws SQLException {
    try (Connection con = getConn(queryResultFormat);
        Statement statement = con.createStatement()) {
      try {
        statement.execute("create or replace table mytab(id int)");
        try (PreparedStatement pstatement =
            con.prepareStatement("insert into mytab(id) values (?)")) {
          pstatement.setLong(1, 4);
          pstatement.addBatch();
          pstatement.setLong(1, 5);
          pstatement.addBatch();
          pstatement.setLong(1, 6);
          pstatement.addBatch();
          pstatement.executeLargeBatch();
          con.commit();
          try (ResultSet resultSet = statement.executeQuery("select * from mytab")) {
            assertTrue(resultSet.next());
            assertEquals(4, resultSet.getInt(1));
          }
        }
      } finally {
        statement.execute("drop table if exists mytab");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testRemoveExtraDescribeCalls(String queryResultFormat) throws SQLException {
    String queryId1 = null;
    String queryId2 = null;
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table test_uuid_with_bind(c1 number)");
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into test_uuid_with_bind values (?)")) {
          preparedStatement.setInt(1, 5);
          assertEquals(1, preparedStatement.executeUpdate());
          queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
          // Calling getMetadata() should no longer require an additional server call because we
          // have
          // the
          // metadata form the executeUpdate
          queryId2 =
              preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
          // Assert the query IDs are the same. This will be the case if there is no additional
          // describe
          // call for getMetadata().
          assertEquals(queryId1, queryId2);

          preparedStatement.addBatch();
        }
        try (PreparedStatement preparedStatement =
            connection.prepareStatement("select * from test_uuid_with_bind where c1 = ?")) {
          assertFalse(
              preparedStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
          preparedStatement.setInt(1, 5);

          try (ResultSet resultSet = preparedStatement.executeQuery()) {
            assertThat(resultSet.next(), is(true));
            queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
            queryId2 =
                preparedStatement
                    .getMetaData()
                    .unwrap(SnowflakeResultSetMetaData.class)
                    .getQueryID();
            String queryId3 = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
            // Assert all 3 query IDs are the same because only 1 server call was executed
            assertEquals(queryId1, queryId2);
            assertEquals(queryId1, queryId3);
          }
        }
      } finally {
        statement.execute("drop table if exists test_uuid_with_bind");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testRemoveExtraDescribeCallsSanityCheck(String queryResultFormat)
      throws SQLException {
    String queryId1;
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(
              "create or replace table test_uuid_with_bind(c1 number, c2 string)")) {
        preparedStatement.execute();
        queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
      }
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("insert into test_uuid_with_bind values (?, ?)")) {
        assertFalse(
            preparedStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
        preparedStatement.setInt(1, 5);
        preparedStatement.setString(2, "hello");
        preparedStatement.addBatch();
        preparedStatement.setInt(1, 7);
        preparedStatement.setString(2, "hello1");
        preparedStatement.addBatch();
        String queryId2 =
            preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
        // These query IDs should not match because they are from 2 different prepared statements
        assertNotEquals(queryId1, queryId2);
        preparedStatement.executeBatch();
        String queryId3 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
        // Another execute call was created, so prepared statement has new query ID
        assertNotEquals(queryId2, queryId3);
        // Calling getMetadata() should no longer require an additional server call because we
        // have
        // the
        // metadata form the executeUpdate
        String queryId4 =
            preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
        // Assert the query IDs for the 2 identical getMetadata() calls are the same. They should
        // match
        // since metadata no longer gets overwritten after successive query calls.
        assertEquals(queryId2, queryId4);
        connection.createStatement().execute("drop table if exists test_uuid_with_bind");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testAlreadyDescribedMultipleResults(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.execute();
        // The statement above has already been described since it has been executed
        assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(selectSQL)) {
        // Assert the statement, once it has been re-created, has already described set to false
        assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
        prepStatement.setInt(1, 1);
        try (ResultSet rs = prepStatement.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
        }
      }
      try (PreparedStatement prepStatement = connection.prepareStatement(selectAllSQL)) {
        // Assert the statement, once it has been re-created, has already described set to false
        assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
        try (ResultSet rs = prepStatement.executeQuery()) {
          assertTrue(rs.next());
          assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
        }
      }
    }
  }

  /**
   * Test that consecutive batch inserts can occur. See SNOW-830719. Fixes regression caused by
   * SNOW-762522.
   *
   * @throws Exception
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testConsecutiveBatchInsertError(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("create or replace table testStageArrayBind(c1 integer, c2 string)");
        try (PreparedStatement prepStatement =
            connection.prepareStatement("insert into testStageArrayBind values (?, ?)")) {
          // Assert to begin with that before the describe call, array binding is not supported
          assertFalse(
              prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
          assertFalse(
              prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
          // Insert enough rows to hit the default binding array threshold
          for (int i = 0; i < 35000; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setString(2, "test" + i);
            prepStatement.addBatch();
          }
          prepStatement.executeBatch();
          // After executing the first batch, verify that array bind support is still true
          assertTrue(
              prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
          for (int i = 0; i < 35000; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setString(2, "test" + i);
            prepStatement.addBatch();
          }
          prepStatement.executeBatch();
          // After executing the second batch, verify that array bind support is still true
          assertTrue(
              prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
        }
      } finally {
        statement.execute("drop table if exists testStageArrayBind");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testToString(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        PreparedStatement prepStatement =
            connection.prepareStatement("select current_version() --testing toString()")) {

      // Query ID is going to be null since we didn't execute the statement yet
      assertEquals(
          "select current_version() --testing toString() - Query ID: null",
          prepStatement.toString());

      prepStatement.executeQuery();
      assertTrue(
          prepStatement
              .toString()
              .matches(
                  "select current_version\\(\\) --testing toString\\(\\) - Query ID: (\\d|\\w)+(-(\\d|\\w)+)+$"));
    }
  }
}
