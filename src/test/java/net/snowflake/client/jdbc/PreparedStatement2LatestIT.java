/*
 * Copyright (c) 2012-2020 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
}
