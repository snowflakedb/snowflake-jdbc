/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.PreparedStatement1IT.bindOneParamSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

import java.sql.*;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * PreparedStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still are not applicable. If it is applicable, move tests to PreparedStatement2IT so
 * that both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryStatement.class)
public class PreparedStatement2LatestIT extends PreparedStatement0IT {
  public PreparedStatement2LatestIT() {
    super("json");
  }

  PreparedStatement2LatestIT(String queryFormat) {
    super(queryFormat);
  }

  @Test
  public void testPrepareUDTF() throws Exception {
    try (Connection connection = init()) {
      try {
        connection
            .createStatement()
            .execute("create or replace table employee(id number, address text)");
        connection
            .createStatement()
            .execute(
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
          prepStatement.execute();
          Assert.fail();
        } catch (SQLException e) {
          // failed because argument type did not match
          Assert.assertThat(e.getErrorCode(), is(1044));
        }

        // create a udf with same name but different arguments and return type
        connection
            .createStatement()
            .execute(
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
        connection
            .createStatement()
            .execute("drop function if exists employee_detail(number, text)");
        connection.createStatement().execute("drop function if exists employee_detail(text, text)");
      }
    }
  }

  /**
   * SNOW-88426: skip bind parameter index check if prepare fails and defer the error checks to
   * execute
   */
  @Test
  public void testSelectWithBinding() throws Throwable {
    try (Connection connection = init()) {
      connection
          .createStatement()
          .execute("create or replace table TESTNULL(created_time timestamp_ntz, mid int)");
      PreparedStatement ps;
      ResultSet rs;
      try {
        // skip bind parameter index check if prepare fails and defer the error checks to execute
        ps =
            connection.prepareStatement(
                "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?, 3) and MID = ?");
        ps.setObject(1, 0);
        ps.setObject(2, null);
        rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();

        // describe is success and do the index range check
        ps =
            connection.prepareStatement(
                "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?::NUMBER, 3) and MID = ?");
        ps.setObject(1, 0);
        ps.setObject(2, null);

        rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();

      } finally {
        connection.createStatement().execute("drop table if exists TESTNULL");
      }
    }
  }

  @Test
  public void testLimitBind() throws SQLException {
    try (Connection connection = init()) {
      String stmtStr = "select seq4() from table(generator(rowcount=>100)) limit ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr)) {
        prepStatement.setInt(1, 10);
        prepStatement.executeQuery(); // ensure no error is raised.
      }
    }
  }

  /** SNOW-31746 */
  @Test
  public void testConstOptLimitBind() throws SQLException {
    try (Connection connection = init()) {
      String stmtStr = "select 1 limit ? offset ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr)) {
        prepStatement.setInt(1, 10);
        prepStatement.setInt(2, 0);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          resultSet.next();
          assertThat(resultSet.getInt(1), is(1));
          assertThat(resultSet.next(), is(false));
        }
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testTableFuncBindInput() throws SQLException {
    try (Connection connection = init()) {
      try (PreparedStatement prepStatement = connection.prepareStatement(tableFuncSQL)) {
        prepStatement.setInt(1, 2);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  @Test
  public void testExecuteLargeBatch() throws SQLException {
    try (Connection con = init()) {
      try (Statement statement = con.createStatement()) {
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
            resultSet.next();
            assertEquals(4, resultSet.getInt(1));
          }
          statement.execute("drop table if exists mytab");
        }
      }
    }
  }

  @Test
  public void testRemoveExtraDescribeCalls() throws SQLException {
    Connection connection = init();
    Statement statement = connection.createStatement();
    statement.execute("create or replace table test_uuid_with_bind(c1 number)");

    PreparedStatement preparedStatement =
        connection.prepareStatement("insert into test_uuid_with_bind values (?)");
    preparedStatement.setInt(1, 5);
    assertEquals(1, preparedStatement.executeUpdate());
    String queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
    // Calling getMetadata() should no longer require an additional server call because we have the
    // metadata form the executeUpdate
    String queryId2 =
        preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
    // Assert the query IDs are the same. This will be the case if there is no additional describe
    // call for getMetadata().
    assertEquals(queryId1, queryId2);

    preparedStatement.addBatch();

    preparedStatement =
        connection.prepareStatement("select * from test_uuid_with_bind where c1 = ?");
    assertFalse(preparedStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
    preparedStatement.setInt(1, 5);

    ResultSet resultSet = preparedStatement.executeQuery();
    assertThat(resultSet.next(), is(true));
    queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
    queryId2 =
        preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
    String queryId3 = resultSet.unwrap(SnowflakeResultSet.class).getQueryID();
    // Assert all 3 query IDs are the same because only 1 server call was executed
    assertEquals(queryId1, queryId2);
    assertEquals(queryId1, queryId3);

    resultSet.close();
    preparedStatement.close();

    statement.execute("drop table if exists test_uuid_with_bind");
    connection.close();
  }

  @Test
  public void testRemoveExtraDescribeCallsSanityCheck() throws SQLException {
    Connection connection = init();
    PreparedStatement preparedStatement =
        connection.prepareStatement(
            "create or replace table test_uuid_with_bind(c1 number, c2 string)");
    preparedStatement.execute();
    String queryId1 = preparedStatement.unwrap(SnowflakePreparedStatement.class).getQueryID();
    preparedStatement =
        connection.prepareStatement("insert into test_uuid_with_bind values (?, ?)");
    assertFalse(preparedStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
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
    // Calling getMetadata() should no longer require an additional server call because we have the
    // metadata form the executeUpdate
    String queryId4 =
        preparedStatement.getMetaData().unwrap(SnowflakeResultSetMetaData.class).getQueryID();
    // Assert the query IDs for the 2 identical getMetadata() calls are the same. They should match
    // since metadata no longer gets overwritten after successive query calls.
    assertEquals(queryId2, queryId4);

    connection.createStatement().execute("drop table if exists test_uuid_with_bind");
    preparedStatement.close();
    connection.close();
  }

  @Test
  public void testAlreadyDescribedMultipleResults() throws SQLException {
    Connection connection = init();
    PreparedStatement prepStatement = connection.prepareStatement(insertSQL);
    bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
    prepStatement.execute();
    // The statement above has already been described since it has been executed
    assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
    prepStatement = connection.prepareStatement(selectSQL);
    // Assert the statement, once it has been re-created, has already described set to false
    assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
    prepStatement.setInt(1, 1);
    ResultSet rs = prepStatement.executeQuery();
    assertTrue(rs.next());
    assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
    prepStatement = connection.prepareStatement(selectAllSQL);
    // Assert the statement, once it has been re-created, has already described set to false
    assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
    rs = prepStatement.executeQuery();
    assertTrue(rs.next());
    assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
  }

  /**
   * Test that consecutive batch inserts can occur. See SNOW-830719. Fixes regression caused by
   * SNOW-762522.
   *
   * @throws Exception
   */
  @Test
  public void testConsecutiveBatchInsertError() throws SQLException {
    try (Connection connection = init()) {
      connection
          .createStatement()
          .execute("create or replace table testStageArrayBind(c1 integer, c2 string)");
      PreparedStatement prepStatement =
          connection.prepareStatement("insert into testStageArrayBind values (?, ?)");
      // Assert to begin with that before the describe call, array binding is not supported
      assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isAlreadyDescribed());
      assertFalse(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
      // Insert enough rows to hit the default binding array threshold
      for (int i = 0; i < 35000; i++) {
        prepStatement.setInt(1, i);
        prepStatement.setString(2, "test" + i);
        prepStatement.addBatch();
      }
      prepStatement.executeBatch();
      // After executing the first batch, verify that array bind support is still true
      assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
      for (int i = 0; i < 35000; i++) {
        prepStatement.setInt(1, i);
        prepStatement.setString(2, "test" + i);
        prepStatement.addBatch();
      }
      prepStatement.executeBatch();
      // After executing the second batch, verify that array bind support is still true
      assertTrue(prepStatement.unwrap(SnowflakePreparedStatementV1.class).isArrayBindSupported());
    }
  }

  @Test
  public void testToString() throws SQLException {
    try (Connection connection = init()) {
      PreparedStatement prepStatement =
          connection.prepareStatement("select current_version() --testing toString()");

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
