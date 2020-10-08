/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.PreparedStatement1IT.bindOneParamSet;
import static org.junit.Assert.*;

import java.sql.*;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * PreparedStatement integration tests for the latest JDBC driver. This doesn't work for the oldest
 * supported driver. Revisit this tests whenever bumping up the oldest supported driver to examine
 * if the tests still are not applicable. If it is applicable, move tests to PreparedStatement1IT so
 * that both the latest and oldest supported driver run the tests.
 */
@Category(TestCategoryStatement.class)
public class PreparedStatement1LatestIT extends PreparedStatement0IT {
  public PreparedStatement1LatestIT() {
    super("json");
  }

  PreparedStatement1LatestIT(String queryResultFormat) {
    super(queryResultFormat);
  }

  @Test
  public void testPrepStWithCacheEnabled() throws SQLException {
    try (Connection connection = init()) {
      // ensure enable the cache result use
      connection.createStatement().execute(enableCacheReuse);

      try (PreparedStatement prepStatement = connection.prepareStatement(insertSQL)) {
        bindOneParamSet(prepStatement, 1, 1.22222, (float) 1.2, "test", 12121212121L, (short) 12);
        prepStatement.execute();
        prepStatement.execute();
        bindOneParamSet(prepStatement, 100, 1.2222, (float) 1.2, "testA", 12122L, (short) 12);
        prepStatement.execute();
      }

      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select * from test_prepst")) {
        resultSet.next();
        assertEquals(resultSet.getInt(1), 1);
        resultSet.next();
        assertEquals(resultSet.getInt(1), 1);
        resultSet.next();
        assertEquals(resultSet.getInt(1), 100);
      }

      try (PreparedStatement prepStatement =
          connection.prepareStatement("select id, id + ? from test_prepst where id  = ?")) {
        prepStatement.setInt(1, 1);
        prepStatement.setInt(2, 1);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          resultSet.next();
          assertEquals(resultSet.getInt(2), 2);
          prepStatement.setInt(1, 1);
          prepStatement.setInt(2, 100);
        }
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          resultSet.next();
          assertEquals(resultSet.getInt(2), 101);
        }
      }
      try (PreparedStatement prepStatement =
          connection.prepareStatement(
              "select seq4() from table(generator(rowcount=>100)) limit ?")) {
        prepStatement.setInt(1, 1);

        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
          prepStatement.setInt(1, 3);
        }
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertTrue(resultSet.next());
          assertFalse(resultSet.next());
        }
      }
    }
  }
  /**
   * Test to ensure it's possible to upload Time values via stage array binding and get proper
   * values back (SNOW-194437)
   *
   * <p>Ignored on GitHub Action because CLIENT_STAGE_ARRAY_BINDING_THRESHOLD parameter is not
   * available to customers so cannot be set when running on Github Action
   *
   * @throws SQLException arises if any exception occurs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testInsertStageArrayBindWithTime() throws SQLException {
    try (Connection connection = init()) {
      Statement statement = connection.createStatement();
      statement.execute("alter session set CLIENT_STAGE_ARRAY_BINDING_THRESHOLD=2");
      statement.execute("create or replace table testStageBindTime (c1 time, c2 time)");
      PreparedStatement prepSt =
          connection.prepareStatement("insert into testStageBindTime values (?, ?)");
      Time[][] timeValues = {
        {new Time(0), new Time(1)},
        {new Time(1000), new Time(Integer.MAX_VALUE)},
        {new Time(123456), new Time(55555)},
        {Time.valueOf("01:02:00"), new Time(-100)},
      };
      for (Time[] value : timeValues) {
        prepSt.setTime(1, value[0]);
        prepSt.setTime(2, value[1]);
        prepSt.addBatch();
      }
      prepSt.executeBatch();
      // check results
      ResultSet rs = statement.executeQuery("select * from testStageBindTime");
      for (Time[] timeValue : timeValues) {
        rs.next();
        assertEquals(timeValue[0].toString(), rs.getTime(1).toString());
        assertEquals(timeValue[1].toString(), rs.getTime(2).toString());
      }
      rs.close();
      statement.execute("drop table if exists testStageBindTime");
      statement.execute("alter session unset CLIENT_STAGE_ARRAY_BINDING_THRESHOLD");
      statement.close();
    }
  }
}
