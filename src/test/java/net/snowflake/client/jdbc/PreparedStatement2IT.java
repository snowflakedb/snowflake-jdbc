/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import com.google.common.collect.Sets;
import net.snowflake.client.ConditionalIgnoreRule;
import net.snowflake.client.RunningOnGithubAction;
import net.snowflake.client.category.TestCategoryStatement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(TestCategoryStatement.class)
public class PreparedStatement2IT extends PreparedStatement0IT
{
  @Before
  public void setUp() throws SQLException
  {
    super.setUp();
  }

  @After
  public void tearDown() throws SQLException
  {
    super.tearDown();
  }

  public PreparedStatement2IT()
  {
    super("json");
  }

  PreparedStatement2IT(String queryFormat)
  {
    super(queryFormat);
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testStageBatchDates() throws SQLException
  {
    try (Connection connection = init())
    {
      Date dEpoch = new Date(0);
      Date dAfterEpoch = new Date(24 * 60 * 60 * 1000);
      Date dBeforeEpoch = new Date(-1 * 24 * 60 * 60 * 1000);
      Date dNow = new Date(System.currentTimeMillis());
      Date dLeapYear = new Date(951782400000L);
      Date dFuture = new Date(32503680000000L);
      Date[] dates = new Date[]{dEpoch, dAfterEpoch, dBeforeEpoch, dNow, dLeapYear, dFuture};
      int[] countResult;

      try
      {
        connection.createStatement().execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
        try (PreparedStatement prepStatement = connection.prepareStatement(
            "INSERT INTO test_prepst_date(id, d)  VALUES(?,?)"))
        {

          // First, run with non-stage binding
          connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
          for (int i = 0; i < dates.length; i++)
          {
            prepStatement.setInt(1, i);
            prepStatement.setDate(2, dates[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult)
          {
            assertEquals(1, res);
          }

          Date[] nonStageResult = new Date[dates.length];
          ResultSet rsNonStage =
              connection.createStatement().executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC");
          for (int i = 0; i < nonStageResult.length; i++)
          {
            rsNonStage.next();
            nonStageResult[i] = rsNonStage.getDate(2);
          }

          connection.createStatement().execute("DELETE FROM test_prepst_date WHERE 1=1");

          // Now, run with stage binding
          connection.createStatement().execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
          for (int i = 0; i < dates.length; i++)
          {
            prepStatement.setInt(1, i);
            prepStatement.setDate(2, dates[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult)
          {
            assertEquals(1, res);
          }

          Date[] stageResult = new Date[dates.length];
          ResultSet rsStage =
              connection.createStatement().executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC");
          for (int i = 0; i < stageResult.length; i++)
          {
            rsStage.next();
            stageResult[i] = rsStage.getDate(2);
          }

          for (int i = 0; i < dates.length; i++)
          {
            assertEquals("Stage binding date should match non-stage binding date",
                         nonStageResult[i], stageResult[i]);
          }
        }
      }
      finally
      {
        connection.createStatement().execute("DROP TABLE IF EXISTS test_prepst_date");
      }
    }
  }

  @Test
  public void testLimitBind() throws SQLException
  {
    try (Connection connection = init())
    {
      String stmtStr = "select seq4() from table(generator(rowcount=>100)) limit ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr))
      {
        prepStatement.setInt(1, 10);
        prepStatement.executeQuery(); // ensure no error is raised.
      }
    }
  }

  /**
   * SNOW-31746
   */
  @Test
  public void testConstOptLimitBind() throws SQLException
  {
    try (Connection connection = init())
    {
      String stmtStr = "select 1 limit ? offset ?";
      try (PreparedStatement prepStatement = connection.prepareStatement(stmtStr))
      {
        prepStatement.setInt(1, 10);
        prepStatement.setInt(2, 0);
        try (ResultSet resultSet = prepStatement.executeQuery())
        {
          resultSet.next();
          assertThat(resultSet.getInt(1), is(1));
          assertThat(resultSet.next(), is(false));
        }
      }
    }
  }

  @Test
  public void testBindWithNullValue() throws SQLException
  {
    try (Connection connection = init())
    {
      connection.createStatement().execute(
          "create or replace table testBindNull(cola date, colb time, colc timestamp, cold number)");

      try (PreparedStatement prepStatement = connection.prepareStatement(
          "insert into testBindNull values (?, ?, ?, ?)"))
      {
        prepStatement.setDate(1, null);
        prepStatement.setTime(2, null);
        prepStatement.setTimestamp(3, null);
        prepStatement.setBigDecimal(4, null);
        prepStatement.addBatch();
        prepStatement.executeBatch();
        try (ResultSet resultSet = connection.createStatement().executeQuery("select * from testBindNull"))
        {
          resultSet.next();
          Date date = resultSet.getDate(1);
          assertNull(date);
          assertTrue(resultSet.wasNull());

          Time time = resultSet.getTime(2);
          assertNull(time);
          assertTrue(resultSet.wasNull());

          Timestamp timestamp = resultSet.getTimestamp(3);
          assertNull(timestamp);
          assertTrue(resultSet.wasNull());

          BigDecimal bg = resultSet.getBigDecimal(4);
          assertNull(bg);
          assertTrue(resultSet.wasNull());
        }
        connection.createStatement().execute("TRUNCATE table testbindnull");
        prepStatement.setDate(1, null, Calendar.getInstance());
        prepStatement.setTime(2, null, Calendar.getInstance());
        prepStatement.setTimestamp(3, null, Calendar.getInstance());
        prepStatement.setBigDecimal(4, null);

        prepStatement.addBatch();
        prepStatement.executeBatch();

        try (ResultSet resultSet = connection.createStatement().executeQuery("select * from testBindNull"))
        {
          resultSet.next();
          Date date = resultSet.getDate(1);
          assertNull(date);
          assertTrue(resultSet.wasNull());

          Time time = resultSet.getTime(2);
          assertNull(time);
          assertTrue(resultSet.wasNull());

          Timestamp timestamp = resultSet.getTimestamp(3);
          assertNull(timestamp);
          assertTrue(resultSet.wasNull());
        }
      }
    }
  }

  @Test
  public void testPrepareDDL() throws SQLException
  {
    try (Connection connection = init())
    {
      try
      {
        try (PreparedStatement prepStatement = connection.prepareStatement(
            "create or replace table testprepareddl(cola number)"))
        {
          prepStatement.execute();
        }
        try (ResultSet resultSet = connection.createStatement().executeQuery("show tables like 'testprepareddl'"))
        {
          // result should only have one row since table is created
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.next(), is(false));
        }
      }
      finally
      {
        connection.createStatement().execute("drop table if exists testprepareddl");
      }
    }
  }

  @Test
  public void testPrepareSCL() throws SQLException
  {
    try (Connection connection = init())
    {
      try (PreparedStatement prepStatement = connection.prepareStatement("use SCHEMA  PUBLIC"))
      {
        prepStatement.execute();
      }
      try (ResultSet resultSet = connection.createStatement().executeQuery("select current_schema()"))
      {
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString(1), is("PUBLIC"));
      }
    }
  }

  @Test
  public void testPrepareTCL() throws SQLException
  {
    try (Connection connection = init())
    {
      connection.setAutoCommit(false);
      String[] testCases = {"BEGIN", "COMMIT"};

      for (String testCase : testCases)
      {
        try (PreparedStatement prepStatement = connection.prepareStatement(testCase))
        {
          try (ResultSet resultSet = prepStatement.executeQuery())
          {
            resultSet.next();
            assertThat(resultSet.getString(1), is("Statement executed successfully."));
          }
        }
      }
    }
  }

  @Test
  public void testPrepareShowCommand() throws SQLException
  {
    try (Connection connection = init())
    {
      try (PreparedStatement prepStatement = connection.prepareStatement("show databases"))
      {
        try (ResultSet resultSet = prepStatement.executeQuery())
        {
          assertTrue(resultSet.next());
        }
      }
    }
  }

  /**
   * This test simulates the prepare timeout. When combine
   * describe and execute, combine thread will wait max to
   * 1 seconds before client coming back with execution request.
   * Sleep thread for 5 second will leads to gs recompiling the statement.
   *
   * @throws SQLException         Will be thrown if any of driver calls fail
   * @throws InterruptedException Will be thrown if the sleep is interrupted
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testPrepareTimeout() throws SQLException, InterruptedException
  {
    try (Connection adminCon = getSnowflakeAdminConnection())
    {
      adminCon.createStatement().execute(
          "alter system set enable_combined_describe=true");
      try
      {
        try (Connection connection = init())
        {
          connection.createStatement().execute("create or replace table t(c1 string) as select 1");
          connection.createStatement().execute(
              "alter session set jdbc_enable_combined_describe=true");
          try (PreparedStatement prepStatement = connection.prepareStatement(
              "select c1 from t order by c1 limit 1"))
          {
            Thread.sleep(5000);
            try (ResultSet resultSet = prepStatement.executeQuery())
            {
              resultSet.next();
              assertThat(resultSet.getInt(1), is(1));
            }
          }
          connection.createStatement().execute("drop table if exists t");
        }
      }
      finally
      {
        adminCon.createStatement().execute(
            "alter system set enable_combined_describe=default");
      }
    }
  }

  /**
   * Test case to make sure 2 non null bind refs was not constant folded into one
   */
  @Test
  public void testSnow36284() throws Exception
  {
    Connection connection = init();

    String query = "select * from (values ('a'), ('b')) x where x.COLUMN1 in (?,?);";
    PreparedStatement preparedStatement = connection.prepareStatement(
        query);
    preparedStatement.setString(1, "a");
    preparedStatement.setString(2, "b");
    ResultSet rs = preparedStatement.executeQuery();
    int rowcount = 0;
    Set<String> valuesReturned = Sets.newHashSetWithExpectedSize(2);
    while (rs.next())
    {
      rowcount++;
      valuesReturned.add(rs.getString(1));
    }
    assertEquals("Should get back 2 rows", 2, rowcount);
    assertEquals("", valuesReturned, Sets.newHashSet("a", "b"));
  }

  /**
   * Test for coalesce with bind and null arguments in a prepared statement
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testSnow35923() throws Exception
  {
    try (Connection connection = init())
    {
      connection.createStatement().execute("alter session set "
                                           + "optimizer_eliminate_scans_for_constant_select=false");
      connection.createStatement().execute("create or replace table inc(a int, b int)");
      connection.createStatement().execute("insert into inc(a, b) values (1, 2), "
                                           + "(NULL, 4), (5,NULL), (7,8)");
      // Query used to cause an incident.
      PreparedStatement preparedStatement = connection.prepareStatement(
          "SELECT coalesce(?, NULL) from inc;");
      preparedStatement.setInt(1, 0);
      ResultSet rs = preparedStatement.executeQuery();
    }
  }

  /**
   * Tests binding of object literals, including binding with object names as
   * well as binding with object IDs
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testBindObjectLiteral() throws Exception
  {
    try (Connection conn = init())
    {
      Statement stmt = conn.createStatement();

      String sqlText = "create or replace table identifier(?) (c1 number)";
      SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      String t1 = "bindObjectTable1";
      // Bind the table name
      pStmt.setString(1, t1);
      ResultSet result = pStmt.executeQuery();

      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" +
                   t1 + "')):entityId;");
      result = stmt.getResultSet();

      long t1Id = 0;
      if (result.next())
      {
        t1Id = Long.valueOf(result.getString(1));
      }

      assertTrue(t1Id != 0);

      // Mix of object literal binds and value binds
      sqlText = "insert into identifier(?) values (1), (2), (3)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      // Bind by object IDs
      pStmt.setLong(1, t1Id);

      result = pStmt.executeQuery();

      // Perform some selection
      sqlText = "select * from identifier(?) order by 1";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t1);
      result = pStmt.executeQuery();
      // Verify 3 rows have been inserted
      for (int i = 0; i < 3; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Alter Table
      sqlText = "alter table identifier(?) add column c2 number";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t1Id);
      result = pStmt.executeQuery();

      // Describe
      sqlText = "desc table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t1);
      result = pStmt.executeQuery();
      // Verify two columns have been created
      for (int i = 0; i < 2; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Create another table
      String t2 = "bindObjectTable2";
      sqlText = "create or replace table identifier(?) (c1 number)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t2);
      result = pStmt.executeQuery();

      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" +
                   t2 + "')):entityId;");
      result = stmt.getResultSet();

      long t2Id = 0;
      if (result.next())
      {
        t2Id = Long.valueOf(result.getString(1));
      }

      assertTrue(t2Id != 0);

      // Mix object binds with value binds
      sqlText = "insert into identifier(?) values (?), (?), (?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, t2);
      pStmt.setInt(2, 1);
      pStmt.setInt(3, 2);
      pStmt.setInt(4, 3);
      result = pStmt.executeQuery();

      // Verify that 3 rows have been inserted
      sqlText = "select * from identifier(?) order by 1";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t2Id);
      result = pStmt.executeQuery();
      for (int i = 0; i < 3; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Multiple Object Binds
      sqlText = "select t2.c1 from identifier(?) as t1, identifier(?) as t2 " +
                "where t1.c1 = t2.c1 and t1.c1 > (?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setString(1, t1);
      pStmt.setLong(2, t2Id);
      pStmt.setInt(3, 1);
      result = pStmt.executeQuery();
      for (int i = 0; i < 2; i++)
      {
        assertTrue(result.next());
      }
      assertFalse(result.next());

      // Drop Tables
      sqlText = "drop table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setString(1, "bindObjectTable1");
      result = pStmt.executeQuery();

      sqlText = "drop table identifier(?)";
      pStmt = (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText);
      pStmt.setParameter("resolve_object_ids", true);
      pStmt.setLong(1, t2Id);
      result = pStmt.executeQuery();

      // Verify that the tables have been dropped
      stmt.execute("show tables like 'bindobjecttable%'");
      result = stmt.getResultSet();
      assertFalse(result.next());
    }
  }

  @Test
  public void testBindTimestampTZViaString() throws SQLException
  {
    try (Connection connection = init())
    {
      connection.createStatement().execute("alter session set timestamp_tz_output_format='YYYY-MM" +
                                           "-DD HH24:MI:SS.FF9 TZHTZM'");
      connection.createStatement().execute("create or replace  table testbindtstz(cola timestamp_tz)");

      try (PreparedStatement preparedStatement = connection.prepareStatement(
          "insert into testbindtstz values(?)"))
      {
        preparedStatement.setString(1, "2017-11-30T18:17:05.123456789+08:00");
        int count = preparedStatement.executeUpdate();
        assertThat(count, is(1));
      }
      try (ResultSet resultSet = connection.createStatement().executeQuery("select * from testbindtstz"))
      {
        assertTrue(resultSet.next());
        assertThat(resultSet.getString(1), is("2017-11-30 18:17:05.123456789 +0800"));
      }
      connection.createStatement().execute("drop table if exists testbindtstz");
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testTableFuncBindInput() throws SQLException
  {
    try (Connection connection = init())
    {
      try (PreparedStatement prepStatement = connection.prepareStatement(tableFuncSQL))
      {
        prepStatement.setInt(1, 2);
        try (ResultSet resultSet = prepStatement.executeQuery())
        {
          assertEquals(2, getSizeOfResultSet(resultSet));
        }
      }
    }
  }

  /**
   * Ensures binding a string type with TIMESTAMP_TZ works. The customer
   * has to use the specific timestamp format:
   * YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM
   */
  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testBindTimestampTZViaStringBatch() throws SQLException
  {
    try (Connection connection = init())
    {
      connection.createStatement().execute(
          "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
      connection.createStatement().execute(
          "create or replace table testbindtstz(cola timestamp_tz, colb timestamp_ntz)");

      try (PreparedStatement preparedStatement = connection.prepareStatement(
          "insert into testbindtstz values(?,?)"))
      {

        preparedStatement.setString(1, "2017-11-30 18:17:05.123456789 +08:00");
        preparedStatement.setString(2, "2017-11-30 18:17:05.123456789");
        preparedStatement.addBatch();
        preparedStatement.setString(1, "2017-05-03 16:44:42.0");
        preparedStatement.setString(2, "2017-05-03 16:44:42.0");
        preparedStatement.addBatch();
        int[] count = preparedStatement.executeBatch();
        assertThat(count[0], is(1));

        try (
            ResultSet resultSet = connection.createStatement().executeQuery("select * from testbindtstz order by 1 desc"))
        {
          assertTrue(resultSet.next());
          assertThat(resultSet.getString(1), is("Thu, 30 Nov 2017 18:17:05 +0800"));
          assertThat(resultSet.getString(2), is("Thu, 30 Nov 2017 18:17:05 Z"));

          assertTrue(resultSet.next());
          assertThat(resultSet.getString(1), is("Wed, 03 May 2017 16:44:42 -0700"));
          assertThat(resultSet.getString(2), is("Wed, 03 May 2017 16:44:42 Z"));
        }
      }
      connection.createStatement().execute("drop table if exists testbindtstz");
    }
  }

  /**
   * Test to ensure that when SqlBindRef is in a SqlConstantList
   * the sub-expression remover does not treat expressions with different
   * bind values are same expressions. SNOW-41620
   *
   * @throws Exception raises if any error occurs
   */
  @Test
  public void testSnow41620() throws Exception
  {
    try (Connection connection = init())
    {
      // Create a table and insert 3 records
      connection.createStatement().execute(
          "CREATE or REPLACE TABLE SNOW41620 (c1 varchar(20)," + "c2 int" + "  )");
      connection.createStatement().execute(
          "insert into SNOW41620 values('value1', 1), ('value2', 2), ('value3', 3)");

      String PARAMETERIZED_QUERY = "SELECT t0.C1, " +
                                   "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END, " +
                                   "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END " +
                                   "FROM SNOW41620 t0";

      ResultSet bindStmtResultSet;
      try (PreparedStatement pst = connection.prepareStatement(PARAMETERIZED_QUERY))
      {
        // bind values
        pst.setObject(1, "value1");
        pst.setObject(2, "value3");
        pst.setObject(3, "value2");
        pst.setObject(4, "value3");
        bindStmtResultSet = pst.executeQuery();

        // Execute the same query with bind values replaced in the sql
        String DIRECT_QUERY = "SELECT t0.C1, " +
                              "CASE WHEN t0.C1 IN ('value1', 'value3') THEN t0.C2  ELSE null END," +
                              "CASE WHEN t0.C1 IN ('value2', 'value3') THEN t0.C2  ELSE null END " +
                              "FROM SNOW41620 t0";
        try (PreparedStatement pst1 = connection.prepareStatement(DIRECT_QUERY))
        {
          ResultSet directStmtResultSet = pst1.executeQuery();

          checkResultSetEqual(bindStmtResultSet, directStmtResultSet);

          bindStmtResultSet.close();
          directStmtResultSet.close();
        }
      }
    }
  }

  @Test
  public void testSnow50141() throws Exception
  {
    try (Connection connection = init())
    {
      try (PreparedStatement prepStatement = connection.prepareStatement("select 1 where true=?"))
      {
        prepStatement.setObject(1, true);
        try (ResultSet resultSet = prepStatement.executeQuery())
        {
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.getInt(1), is(1));
          assertThat(resultSet.next(), is(false));
        }
      }
    }
  }

  @Test
  public void testPrepareUDTF() throws Exception
  {
    try (Connection connection = init())
    {
      try
      {
        connection.createStatement().execute("create or replace table employee(id number, address text)");
        connection.createStatement().execute("create or replace function employee_detail(sid number, addr text)\n" +
                                             " returns table(id number, address text)\n" +
                                             "LANGUAGE SQL\n" +
                                             "as\n" +
                                             "$$\n" +
                                             "select *\n" +
                                             "from employee\n" +
                                             "where  id=sid\n" +
                                             "$$;");

        // should resolve successfully
        try (PreparedStatement prepStatement = connection.prepareStatement(
            "select * from table(employee_detail(?, ?))"))
        {
          prepStatement.setInt(1, 1);
          prepStatement.setString(2, "abc");
          prepStatement.execute();
        }

        //should resolve successfully
        try (PreparedStatement prepStatement = connection.prepareStatement(
            "select * from table(employee_detail(?, 'abc'))"))

        {
          prepStatement.setInt(1, 1);
          prepStatement.execute();
        }

        try (PreparedStatement prepStatement = connection.prepareStatement(
            "select * from table(employee_detail(?, 123))");)
        {
          // second argument is invalid
          prepStatement.setInt(1, 1);
          prepStatement.execute();
          Assert.fail();
        }
        catch (SQLException e)
        {
          // failed because argument type did not match
          Assert.assertThat(e.getErrorCode(), is(1044));
        }

        // create a udf with same name but different arguments and return type
        connection.createStatement().execute(
            "create or replace function employee_detail(name text , addr text)\n" +
            " returns table(id number)\n" +
            "LANGUAGE SQL\n" +
            "as\n" +
            "$$\n" +
            "select id\n" +
            "from employee\n" +
            "$$;");

        try (PreparedStatement prepStatement = connection.prepareStatement(
            "select * from table(employee_detail(?, 'abc'))"))
        {
          prepStatement.setInt(1, 1);
          prepStatement.execute();
        }
      }
      finally
      {
        connection.createStatement().execute("drop function if exists employee_detail(number, text)");
        connection.createStatement().execute("drop function if exists employee_detail(text, text)");
      }
    }
  }

  /**
   * Check if both the {@link ResultSet} passed in has equivalent data.
   * Checks the toString of the underlying data returned.
   *
   * @param rs1 ResultSet1 to compare
   * @param rs2 ResultSet2 to compare
   * @throws SQLException Will be thrown if any of the Driver calls fail
   */
  private void checkResultSetEqual(ResultSet rs1, ResultSet rs2)
  throws SQLException
  {
    int columns = rs1.getMetaData().getColumnCount();
    assertEquals(
        "Resultsets do not match in the number of columns returned",
        columns, rs2.getMetaData().getColumnCount());

    while (rs1.next() && rs2.next())
    {
      for (int columnIndex = 1; columnIndex <= columns; columnIndex++)
      {
        final Object res1 = rs1.getObject(columnIndex);
        final Object res2 = rs2.getObject(columnIndex);

        assertEquals(
            String.format("%s and %s are not equal values at column %d",
                          res1, res2, columnIndex),
            res1, res2);
      }

      assertEquals(
          "Number of records returned by the results does not match",
          rs1.isLast(), rs2.isLast());
    }
  }

  @Test
  public void testPreparedStatementWithSkipParsing() throws Exception
  {
    try (Connection con = init())
    {
      PreparedStatement stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
          "select 1", true);
      try (ResultSet rs = stmt.executeQuery())
      {
        assertThat(rs.next(), is(true));
        assertThat(rs.getInt(1), is(1));
      }
    }
  }

  @Test
  public void testPreparedStatementWithSkipParsingAndBinding() throws Exception
  {
    try (Connection con = init())
    {
      con.createStatement().execute("create or replace table t(c1 int)");
      try
      {
        try (PreparedStatement stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
            "insert into t(c1) values(?)", true))
        {
          stmt.setInt(1, 123);
          int ret = stmt.executeUpdate();
          assertThat(ret, is(1));
        }
        try (PreparedStatement stmt = con.unwrap(SnowflakeConnectionV1.class).prepareStatement(
            "select * from t", true))
        {
          ResultSet rs = stmt.executeQuery();
          assertThat(rs.next(), is(true));
          assertThat(rs.getInt(1), is(123));
        }
      }
      finally
      {
        con.createStatement().execute("drop table if exists t");
      }
    }
  }

  /**
   * Test prepare mode for to_timestamp(?, 3), compiler right now can not
   * handle this properly. A workaround is added.
   * More specifically, ErrorCode returned for this statement is caught in
   * SnowflakePreparedStatementV1 so that execution can continue
   */
  @Test
  public void testSnow44393() throws Exception
  {
    try (Connection con = init())
    {
      assertFalse(
          con.createStatement().execute("alter session set timestamp_ntz_output_format='YYYY-MM-DD HH24:MI:SS'"));
      try (PreparedStatement stmt = con.prepareStatement("select to_timestamp_ntz(?, 3)"))
      {
        stmt.setBigDecimal(1, new BigDecimal("1261440000000"));
        ResultSet resultSet = stmt.executeQuery();
        resultSet.next();

        String res = resultSet.getString(1);
        assertThat(res, is("2009-12-22 00:00:00"));
      }
    }
  }

  @Test
  @ConditionalIgnoreRule.ConditionalIgnore(condition = RunningOnGithubAction.class)
  public void testAddBatchNumericNullFloatMixed() throws Exception
  {
    try (Connection connection = init())
    {
      for (int threshold = 0; threshold < 2; ++threshold)
      {
        connection.createStatement().execute(
            "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = " +
            threshold); // enable stage bind
        String sql = "insert into TEST_PREPST(col, colB) values(?,?)";

        // Null NUMERIC followed by FLOAT.
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.NUMERIC);
          stmt.addBatch();
          stmt.setInt(1, 2);
          stmt.setObject(2, 4.0f);
          stmt.addBatch();
          stmt.executeBatch();

          // evaluate query ids
          assertTrue(stmt.isWrapperFor(SnowflakePreparedStatement.class));
          String qid1 = stmt.unwrap(SnowflakePreparedStatement.class).getQueryID();
          assertNotNull(qid1);
        }

        // Null CHAR followed by FLOAT. CHAR type is ignored.
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.CHAR);
          stmt.addBatch();
          stmt.setInt(1, 2);
          stmt.setObject(2, 4.0f);
          stmt.addBatch();
          stmt.executeBatch();
        }

        // FLOAT followed by Null NUMERIC. Type for null value is ignored
        // anyway.
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
          stmt.setInt(1, 2);
          stmt.setObject(2, 4.0f);
          stmt.addBatch();
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.NUMERIC);
          stmt.addBatch();
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.BOOLEAN);
          stmt.addBatch();
          stmt.executeBatch();
        }

        // Null NUMERIC followed by FLOAT, but STRING won't be allowed.
        try (PreparedStatement stmt = connection.prepareStatement(sql))
        {
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.NUMERIC);
          stmt.addBatch();
          stmt.setInt(1, 2);
          stmt.setObject(2, 4.0f);
          stmt.addBatch();
          stmt.setInt(1, 1);
          stmt.setObject(2, "test1");
          try
          {
            stmt.addBatch();
            fail("Must fail");
          }
          catch (SnowflakeSQLException ex)
          {
            assertThat("Error code is wrong",
                       ex.getErrorCode(),
                       equalTo(ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode()));
            assertThat("Location",
                       ex.getMessage(), containsString("Column: 2, Row: 3"));
          }
        }
      }
    }
  }

  @Test
  public void testInvalidUsageOfApi() throws Exception
  {
    Connection connection = init();
    final PreparedStatement preparedStatement = connection.prepareStatement(
        "select 1");
    final int expectedCode =
        ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode();

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.executeUpdate("select 1");
      }
    }, expectedCode);

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.execute("select 1");
      }
    }, expectedCode);

    assertException(new RunnableWithSQLException()
    {
      @Override
      public void run() throws SQLException
      {
        preparedStatement.addBatch("select 1");
      }
    }, expectedCode);

  }

  private void assertException(RunnableWithSQLException runnable,
                               int expectedCode)
  {
    try
    {
      runnable.run();
      Assert.fail();
    }
    catch (SQLException e)
    {
      assertThat(e.getErrorCode(), is(expectedCode));
    }
  }

  private interface RunnableWithSQLException
  {
    void run() throws SQLException;
  }

  @Test
  public void testCreatePreparedStatementWithParameters() throws Throwable
  {
    try (Connection connection = init())
    {
      connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      try
      {
        connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        fail("updateable cursor is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }
      connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                  ResultSet.CLOSE_CURSORS_AT_COMMIT);
      try
      {
        connection.prepareStatement("select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                    ResultSet.HOLD_CURSORS_OVER_COMMIT);
        fail("hold cursor over commit is not supported.");
      }
      catch (SQLException ex)
      {
        assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      }

    }
  }

  /**
   * SNOW-88426: skip bind parameter index check if prepare fails and defer the error checks to execute
   */
  @Test
  public void testSelectWithBinding() throws Throwable
  {
    try (Connection connection = init())
    {
      connection.createStatement().execute("create or replace table TESTNULL(created_time timestamp_ntz, mid int)");
      PreparedStatement ps;
      ResultSet rs;
      try
      {
        // skip bind parameter index check if prepare fails and defer the error checks to execute
        ps = connection.prepareStatement(
            "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?, 3) and MID = ?"
        );
        ps.setObject(1, 0);
        ps.setObject(2, null);
        ps.setObject(1000, null); // this won't raise an exception.
        rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();

        // describe is success and do the index range check
        ps = connection.prepareStatement(
            "SELECT 1 FROM TESTNULL WHERE CREATED_TIME = TO_TIMESTAMP(?::NUMBER, 3) and MID = ?"
        );
        ps.setObject(1, 0);
        ps.setObject(2, null);
        ps.setObject(1000, null); // this won't raise an exception.

        rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();

      }
      finally
      {
        connection.createStatement().execute("drop table if exists TESTNULL");
      }
    }
  }

  @Test
  public void testPrepareAndGetMeta() throws SQLException
  {
    try (Connection con = init())
    {
      try (PreparedStatement prepStatement = con.prepareStatement("select 1 where 1 > ?"))
      {
        ResultSetMetaData meta = prepStatement.getMetaData();
        assertThat(meta.getColumnCount(), is(1));
      }
      try (PreparedStatement prepStatement = con.prepareStatement("select 1 where 1 > ?"))
      {
        ParameterMetaData parameterMetaData = prepStatement.getParameterMetaData();
        assertThat(parameterMetaData.getParameterCount(), is(1));
      }
    }
  }
}
