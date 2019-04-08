package net.snowflake.client.jdbc;

import net.snowflake.client.RunningOnTravisCI;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;

import static net.snowflake.client.ConditionalIgnoreRule.ConditionalIgnore;

public class PrepareMultiStmtIT extends BaseJDBCTest
{
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteUpdateCount() throws Exception
  {
    SnowflakeConnectionV1 connection = (SnowflakeConnectionV1) getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");
    statement.execute("create or replace table test_multi_bind(c1 number)");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into test_multi_bind(c1) values(?); insert into " +
        "test_multi_bind values (?), (?)");

    assertThat(preparedStatement.getParameterMetaData().getParameterCount(),
               is(3));

    preparedStatement.setInt(1, 20);
    preparedStatement.setInt(2, 30);
    preparedStatement.setInt(3, 40);

    // first statement
    int updateCount = preparedStatement.executeUpdate();
    assertThat(updateCount, is(1));
    assertThat(preparedStatement.getResultSet(), is(nullValue()));
    assertThat(preparedStatement.getUpdateCount(), is(1));

    // second statement
    assertThat(preparedStatement.getMoreResults(), is(false));
    assertThat(preparedStatement.getUpdateCount(), is(2));

    ResultSet resultSet = statement.executeQuery(
        "select c1 from test_multi_bind order by c1 asc");
    resultSet.next();
    assertThat(resultSet.getInt(1), is(20));
    resultSet.next();
    assertThat(resultSet.getInt(1), is(30));
    resultSet.next();
    assertThat(resultSet.getInt(1), is(40));

    statement.execute("drop table if exists test_multi_bind");

    preparedStatement.close();
    connection.close();
  }

  /**
   * Less bindings than expected in statement
   */
  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteLessBindings() throws Exception
  {
    SnowflakeConnectionV1 connection = (SnowflakeConnectionV1) getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");
    statement.execute("create or replace table test_multi_bind(c1 number)");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into test_multi_bind(c1) values(?); insert into " +
        "test_multi_bind values (?), (?)");

    assertThat(preparedStatement.getParameterMetaData().getParameterCount(),
               is(3));

    preparedStatement.setInt(1, 20);
    preparedStatement.setInt(2, 30);

    // first statement
    try
    {
      preparedStatement.executeUpdate();
      Assert.fail();
    }
    catch (SQLException e)
    {
      // error code comes from xp, which is js execution failed.
      assertThat(e.getErrorCode(), is(100132));
    }

    statement.execute("drop table if exists test_multi_bind");
    preparedStatement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteMoreBindings() throws Exception
  {
    SnowflakeConnectionV1 connection = (SnowflakeConnectionV1) getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");
    statement.execute("create or replace table test_multi_bind(c1 number)");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "insert into test_multi_bind(c1) values(?); insert into " +
        "test_multi_bind values (?), (?)");

    assertThat(preparedStatement.getParameterMetaData().getParameterCount(),
               is(3));

    preparedStatement.setInt(1, 20);
    preparedStatement.setInt(2, 30);
    preparedStatement.setInt(3, 40);
    // 4th binding should be ignored
    preparedStatement.setInt(4, 50);

    // first statement
    int updateCount = preparedStatement.executeUpdate();
    assertThat(updateCount, is(1));
    assertThat(preparedStatement.getResultSet(), is(nullValue()));
    assertThat(preparedStatement.getUpdateCount(), is(1));

    // second statement
    assertThat(preparedStatement.getMoreResults(), is(false));
    assertThat(preparedStatement.getUpdateCount(), is(2));

    ResultSet resultSet = statement.executeQuery(
        "select c1 from test_multi_bind order by c1 asc");
    resultSet.next();
    assertThat(resultSet.getInt(1), is(20));
    resultSet.next();
    assertThat(resultSet.getInt(1), is(30));
    resultSet.next();
    assertThat(resultSet.getInt(1), is(40));

    statement.execute("drop table if exists test_multi_bind");

    preparedStatement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteQueryBindings() throws Exception
  {
    SnowflakeConnectionV1 connection = (SnowflakeConnectionV1) getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "select ?; select ?, ?; select ?, ?, ?");

    assertThat(preparedStatement.getParameterMetaData().getParameterCount(),
               is(6));

    preparedStatement.setInt(1, 10);
    preparedStatement.setInt(2, 20);
    preparedStatement.setInt(3, 30);
    preparedStatement.setInt(4, 40);
    preparedStatement.setInt(5, 50);
    preparedStatement.setInt(6, 60);

    // first statement
    ResultSet resultSet = preparedStatement.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(10));

    // second statement
    assertThat(preparedStatement.getMoreResults(), is(true));
    resultSet = preparedStatement.getResultSet();
    resultSet.next();
    assertThat(resultSet.getInt(1), is(20));
    assertThat(resultSet.getInt(2), is(30));

    // third statement
    assertThat(preparedStatement.getMoreResults(), is(true));
    resultSet = preparedStatement.getResultSet();
    resultSet.next();
    assertThat(resultSet.getInt(1), is(40));
    assertThat(resultSet.getInt(2), is(50));
    assertThat(resultSet.getInt(3), is(60));

    preparedStatement.close();
    connection.close();
  }

  @Test
  @ConditionalIgnore(condition = RunningOnTravisCI.class)
  public void testExecuteQueryNoBindings() throws Exception
  {
    SnowflakeConnectionV1 connection = (SnowflakeConnectionV1) getConnection();
    Statement statement = connection.createStatement();
    statement.execute("alter session set ENABLE_MULTISTATEMENT=true");

    PreparedStatement preparedStatement = connection.prepareStatement(
        "select 10; select 20, 30; select 40, 50, 60");

    assertThat(preparedStatement.getParameterMetaData().getParameterCount(),
               is(0));

    // first statement
    ResultSet resultSet = preparedStatement.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(10));

    // second statement
    assertThat(preparedStatement.getMoreResults(), is(true));
    resultSet = preparedStatement.getResultSet();
    resultSet.next();
    assertThat(resultSet.getInt(1), is(20));
    assertThat(resultSet.getInt(2), is(30));

    // third statement
    assertThat(preparedStatement.getMoreResults(), is(true));
    resultSet = preparedStatement.getResultSet();
    resultSet.next();
    assertThat(resultSet.getInt(1), is(40));
    assertThat(resultSet.getInt(2), is(50));
    assertThat(resultSet.getInt(3), is(60));

    preparedStatement.close();
    connection.close();
  }
}
