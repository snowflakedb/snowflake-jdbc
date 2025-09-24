package net.snowflake.client.jdbc;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Sets;
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
import java.util.TimeZone;
import net.snowflake.client.annotations.DontRunOnGithubActions;
import net.snowflake.client.category.TestTags;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(TestTags.STATEMENT)
public class PreparedStatement2IT extends PreparedStatement0IT {
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testStageBatchDates(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      Date dEpoch = new Date(0);
      Date dAfterEpoch = new Date(24 * 60 * 60 * 1000);
      Date dBeforeEpoch = new Date(-1 * 24 * 60 * 60 * 1000);
      Date dNow = new Date(System.currentTimeMillis());
      Date dLeapYear = new Date(951782400000L);
      Date dFuture = new Date(32503680000000L);
      Date[] dates = new Date[] {dEpoch, dAfterEpoch, dBeforeEpoch, dNow, dLeapYear, dFuture};
      int[] countResult;

      try {
        statement.execute("CREATE OR REPLACE TABLE test_prepst_date (id INTEGER, d DATE)");
        try (PreparedStatement prepStatement =
            connection.prepareStatement("INSERT INTO test_prepst_date(id, d)  VALUES(?,?)")) {

          // First, run with non-stage binding
          statement.execute("ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 0");
          for (int i = 0; i < dates.length; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setDate(2, dates[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult) {
            assertEquals(1, res);
          }

          Date[] nonStageResult = new Date[dates.length];
          try (ResultSet rsNonStage =
              statement.executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC")) {

            for (int i = 0; i < nonStageResult.length; i++) {
              assertTrue(rsNonStage.next());
              nonStageResult[i] = rsNonStage.getDate(2);
            }
          }
          statement.execute("DELETE FROM test_prepst_date WHERE 1=1");

          // Now, run with stage binding
          statement.execute(
              "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage
          // bind
          for (int i = 0; i < dates.length; i++) {
            prepStatement.setInt(1, i);
            prepStatement.setDate(2, dates[i]);
            prepStatement.addBatch();
          }
          countResult = prepStatement.executeBatch();
          for (int res : countResult) {
            assertEquals(1, res);
          }

          Date[] stageResult = new Date[dates.length];
          try (ResultSet rsStage =
              statement.executeQuery("SELECT * FROM test_prepst_date ORDER BY id ASC")) {
            for (int i = 0; i < stageResult.length; i++) {
              assertTrue(rsStage.next());
              stageResult[i] = rsStage.getDate(2);
            }

            for (int i = 0; i < dates.length; i++) {
              assertEquals(
                  nonStageResult[i],
                  stageResult[i],
                  "Stage binding date should match non-stage binding date");
            }
          }
        }
      } finally {
        statement.execute("DROP TABLE IF EXISTS test_prepst_date");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBindWithNullValue(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "create or replace table testBindNull(cola date, colb time, colc timestamp, cold number)");

      try (PreparedStatement prepStatement =
          connection.prepareStatement("insert into testBindNull values (?, ?, ?, ?)")) {
        prepStatement.setDate(1, null);
        prepStatement.setTime(2, null);
        prepStatement.setTimestamp(3, null);
        prepStatement.setBigDecimal(4, null);
        prepStatement.addBatch();
        prepStatement.executeBatch();
        try (ResultSet resultSet = statement.executeQuery("select * from testBindNull")) {
          assertTrue(resultSet.next());
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
        statement.execute("TRUNCATE table testbindnull");
        prepStatement.setDate(1, null, Calendar.getInstance());
        prepStatement.setTime(2, null, Calendar.getInstance());
        prepStatement.setTimestamp(3, null, Calendar.getInstance());
        prepStatement.setBigDecimal(4, null);

        prepStatement.addBatch();
        prepStatement.executeBatch();

        try (ResultSet resultSet = statement.executeQuery("select * from testBindNull")) {
          assertTrue(resultSet.next());
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

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareDDL(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        try (PreparedStatement prepStatement =
            connection.prepareStatement("create or replace table testprepareddl(cola number)")) {
          prepStatement.execute();
        }
        try (ResultSet resultSet = statement.executeQuery("show tables like 'testprepareddl'")) {
          // result should only have one row since table is created
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.next(), is(false));
        }
      } finally {
        statement.execute("drop table if exists testprepareddl");
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareSCL(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement("use SCHEMA  PUBLIC")) {
        prepStatement.execute();
      }
      try (ResultSet resultSet =
          connection.createStatement().executeQuery("select current_schema()")) {
        assertThat(resultSet.next(), is(true));
        assertThat(resultSet.getString(1), is("PUBLIC"));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareTCL(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.setAutoCommit(false);
      String[] testCases = {"BEGIN", "COMMIT"};

      for (String testCase : testCases) {
        try (PreparedStatement prepStatement = connection.prepareStatement(testCase)) {
          try (ResultSet resultSet = prepStatement.executeQuery()) {
            assertTrue(resultSet.next());
            assertThat(resultSet.getString(1), is("Statement executed successfully."));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareShowCommand(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement("show databases")) {
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertTrue(resultSet.next());
        }
      }
    }
  }

  /**
   * This test simulates the prepare timeout. When combine describe and execute, combine thread will
   * wait max to 1 seconds before client coming back with execution request. Sleep thread for 5
   * second will leads to gs recompiling the statement.
   *
   * @throws SQLException Will be thrown if any of driver calls fail
   * @throws InterruptedException Will be thrown if the sleep is interrupted
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testPrepareTimeout(String queryResultFormat)
      throws SQLException, InterruptedException {
    try (Connection adminCon = getSnowflakeAdminConnection();
        Statement adminStatement = adminCon.createStatement()) {
      adminStatement.execute("alter system set enable_combined_describe=true");
      try {
        try (Connection connection = getConn(queryResultFormat);
            Statement statement = connection.createStatement()) {
          statement.execute("create or replace table t(c1 string) as select 1");
          statement.execute("alter session set jdbc_enable_combined_describe=true");
          try (PreparedStatement prepStatement =
              connection.prepareStatement("select c1 from t order by c1 limit 1")) {
            Thread.sleep(5000);
            try (ResultSet resultSet = prepStatement.executeQuery()) {
              assertTrue(resultSet.next());
              assertThat(resultSet.getInt(1), is(1));
            }
          }
          statement.execute("drop table if exists t");
        }
      } finally {
        adminStatement.execute("alter system set enable_combined_describe=default");
      }
    }
  }

  /** Test case to make sure 2 non null bind refs was not constant folded into one */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSnow36284(String queryResultFormat) throws Exception {
    String query = "select * from (values ('a'), ('b')) x where x.COLUMN1 in (?,?);";

    try (Connection connection = getConn(queryResultFormat);
        PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, "a");
      preparedStatement.setString(2, "b");
      try (ResultSet rs = preparedStatement.executeQuery()) {
        int rowcount = 0;
        Set<String> valuesReturned = Sets.newHashSetWithExpectedSize(2);
        while (rs.next()) {
          rowcount++;
          valuesReturned.add(rs.getString(1));
        }
        assertEquals(2, rowcount, "Should get back 2 rows");
        assertEquals(valuesReturned, Sets.newHashSet("a", "b"), "");
      }
    }
  }

  /** Test for coalesce with bind and null arguments in a prepared statement */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testSnow35923(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      statement.execute(
          "alter session set " + "optimizer_eliminate_scans_for_constant_select=false");
      statement.execute("create or replace table inc(a int, b int)");
      statement.execute("insert into inc(a, b) values (1, 2), " + "(NULL, 4), (5,NULL), (7,8)");
      // Query used to cause an incident.
      try (PreparedStatement preparedStatement =
          connection.prepareStatement("SELECT coalesce(?, NULL) from inc;")) {
        preparedStatement.setInt(1, 0);
        try (ResultSet rs = preparedStatement.executeQuery()) {}
      }
    }
  }

  /**
   * Tests binding of object literals, including binding with object names as well as binding with
   * object IDs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBindObjectLiteral(String queryResultFormat) throws Exception {
    long t1Id = 0;
    long t2Id = 0;
    String t1 = null;

    try (Connection conn = getConn(queryResultFormat);
        Statement stmt = conn.createStatement()) {

      String sqlText = "create or replace table identifier(?) (c1 number)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        t1 = "bindObjectTable1";
        // Bind the table name
        pStmt.setString(1, t1);
        try (ResultSet result = pStmt.executeQuery()) {}
      }
      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" + t1 + "')):entityId;");
      try (ResultSet result = stmt.getResultSet()) {
        if (result.next()) {
          t1Id = Long.valueOf(result.getString(1));
        }
        assertTrue(t1Id != 0);
      }

      // Mix of object literal binds and value binds
      sqlText = "insert into identifier(?) values (1), (2), (3)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setParameter("resolve_object_ids", true);
        // Bind by object IDs
        pStmt.setLong(1, t1Id);
        try (ResultSet result = pStmt.executeQuery()) {}
      }
      // Perform some selection
      sqlText = "select * from identifier(?) order by 1";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setString(1, t1);
        try (ResultSet result = pStmt.executeQuery()) {
          // Verify 3 rows have been inserted
          for (int i = 0; i < 3; i++) {
            assertTrue(result.next());
          }
          assertFalse(result.next());
        }
      }
      // Alter Table
      sqlText = "alter table identifier(?) add column c2 number";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setParameter("resolve_object_ids", true);
        pStmt.setLong(1, t1Id);
        try (ResultSet result = pStmt.executeQuery()) {}
      }

      // Describe
      sqlText = "desc table identifier(?)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setString(1, t1);
        try (ResultSet result = pStmt.executeQuery()) {
          // Verify two columns have been created
          for (int i = 0; i < 2; i++) {
            assertTrue(result.next());
          }
          assertFalse(result.next());
        }
      }

      // Create another table
      String t2 = "bindObjectTable2";
      sqlText = "create or replace table identifier(?) (c1 number)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setString(1, t2);
        try (ResultSet result = pStmt.executeQuery()) {}
      }
      // Verify the table has been created and get the table ID
      stmt.execute("select parse_json(system$dict_id('table', '" + t2 + "')):entityId;");
      try (ResultSet result = stmt.getResultSet()) {
        if (result.next()) {
          t2Id = Long.valueOf(result.getString(1));
        }
        assertTrue(t2Id != 0);
      }

      // Mix object binds with value binds
      sqlText = "insert into identifier(?) values (?), (?), (?)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setString(1, t2);
        pStmt.setInt(2, 1);
        pStmt.setInt(3, 2);
        pStmt.setInt(4, 3);
        try (ResultSet result = pStmt.executeQuery()) {}
      }
      // Verify that 3 rows have been inserted
      sqlText = "select * from identifier(?) order by 1";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setParameter("resolve_object_ids", true);
        pStmt.setLong(1, t2Id);
        try (ResultSet result = pStmt.executeQuery()) {
          for (int i = 0; i < 3; i++) {
            assertTrue(result.next());
          }
          assertFalse(result.next());
        }
      }

      // Multiple Object Binds
      sqlText =
          "select t2.c1 from identifier(?) as t1, identifier(?) as t2 "
              + "where t1.c1 = t2.c1 and t1.c1 > (?)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setParameter("resolve_object_ids", true);
        pStmt.setString(1, t1);
        pStmt.setLong(2, t2Id);
        pStmt.setInt(3, 1);
        try (ResultSet result = pStmt.executeQuery()) {
          for (int i = 0; i < 2; i++) {
            assertTrue(result.next());
          }
          assertFalse(result.next());
        }
      }

      // Drop Tables
      sqlText = "drop table identifier(?)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setString(1, "bindObjectTable1");
        try (ResultSet result = pStmt.executeQuery()) {}
      }

      sqlText = "drop table identifier(?)";
      try (SnowflakePreparedStatementV1 pStmt =
          (SnowflakePreparedStatementV1) conn.prepareStatement(sqlText)) {
        pStmt.setParameter("resolve_object_ids", true);
        pStmt.setLong(1, t2Id);
        try (ResultSet result = pStmt.executeQuery()) {}
      }

      // Verify that the tables have been dropped
      stmt.execute("show tables like 'bindobjecttable%'");
      try (ResultSet result = stmt.getResultSet()) {
        assertFalse(result.next());
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testBindTimestampTZViaString(String queryResultFormat) throws SQLException {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "alter session set timestamp_tz_output_format='YYYY-MM" + "-DD HH24:MI:SS.FF9 TZHTZM'");
        statement.execute("create or replace  table testbindtstz(cola timestamp_tz)");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testbindtstz values(?)")) {
          preparedStatement.setString(1, "2017-11-30T18:17:05.123456789+08:00");
          int count = preparedStatement.executeUpdate();
          assertThat(count, is(1));
        }
        try (ResultSet resultSet = statement.executeQuery("select * from testbindtstz")) {
          assertTrue(resultSet.next());
          assertThat(resultSet.getString(1), is("2017-11-30 18:17:05.123456789 +0800"));
        }
      } finally {
        statement.execute("drop table if exists testbindtstz");
      }
    }
  }

  /**
   * Ensures binding a string type with TIMESTAMP_TZ works. The customer has to use the specific
   * timestamp format: YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testBindTimestampTZViaStringBatch(String queryResultFormat) throws SQLException {
    TimeZone originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      try {
        statement.execute(
            "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = 1"); // enable stage bind
        statement.execute(
            "create or replace table testbindtstz(cola timestamp_tz, colb timestamp_ntz)");
        statement.execute(
            "ALTER SESSION SET TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
        statement.execute(
            "ALTER SESSION SET TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");

        try (PreparedStatement preparedStatement =
            connection.prepareStatement("insert into testbindtstz values(?,?)")) {

          preparedStatement.setString(1, "2017-11-30 18:17:05.123456789 +08:00");
          preparedStatement.setString(2, "2017-11-30 18:17:05.123456789");
          preparedStatement.addBatch();
          preparedStatement.setString(1, "2017-05-03 16:44:42.0");
          preparedStatement.setString(2, "2017-05-03 16:44:42.0");
          preparedStatement.addBatch();
          int[] count = preparedStatement.executeBatch();
          assertThat(count[0], is(1));

          try (ResultSet resultSet =
              statement.executeQuery("select * from testbindtstz order by 1 desc")) {
            assertTrue(resultSet.next());
            assertThat(resultSet.getString(1), is("Thu, 30 Nov 2017 18:17:05 +0800"));
            assertThat(resultSet.getString(2), is("Thu, 30 Nov 2017 18:17:05 Z"));

            assertTrue(resultSet.next());
            assertThat(resultSet.getString(1), is("Wed, 03 May 2017 16:44:42 -0700"));
            assertThat(resultSet.getString(2), is("Wed, 03 May 2017 16:44:42 Z"));
          }
        }
      } finally {
        statement.execute("drop table if exists testbindtstz");
      }
    } finally {
      TimeZone.setDefault(originalTimeZone);
    }
  }

  /**
   * Test to ensure that when SqlBindRef is in a SqlConstantList the sub-expression remover does not
   * treat expressions with different bind values are same expressions. SNOW-41620
   *
   * @throws Exception raises if any error occurs
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSnow41620(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat);
        Statement statement = connection.createStatement()) {
      // Create a table and insert 3 records
      statement.execute("CREATE or REPLACE TABLE SNOW41620 (c1 varchar(20)," + "c2 int" + "  )");
      statement.execute("insert into SNOW41620 values('value1', 1), ('value2', 2), ('value3', 3)");

      String PARAMETERIZED_QUERY =
          "SELECT t0.C1, "
              + "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END, "
              + "CASE WHEN t0.C1 IN (?, ?) THEN t0.C2  ELSE null END "
              + "FROM SNOW41620 t0";

      try (PreparedStatement pst = connection.prepareStatement(PARAMETERIZED_QUERY)) {
        // bind values
        pst.setObject(1, "value1");
        pst.setObject(2, "value3");
        pst.setObject(3, "value2");
        pst.setObject(4, "value3");
        try (ResultSet bindStmtResultSet = pst.executeQuery()) {

          // Execute the same query with bind values replaced in the sql
          String DIRECT_QUERY =
              "SELECT t0.C1, "
                  + "CASE WHEN t0.C1 IN ('value1', 'value3') THEN t0.C2  ELSE null END,"
                  + "CASE WHEN t0.C1 IN ('value2', 'value3') THEN t0.C2  ELSE null END "
                  + "FROM SNOW41620 t0";
          try (PreparedStatement pst1 = connection.prepareStatement(DIRECT_QUERY);
              ResultSet directStmtResultSet = pst1.executeQuery()) {
            checkResultSetEqual(bindStmtResultSet, directStmtResultSet);
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSnow50141(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = connection.prepareStatement("select 1 where true=?")) {
        prepStatement.setObject(1, true);
        try (ResultSet resultSet = prepStatement.executeQuery()) {
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.getInt(1), is(1));
          assertThat(resultSet.next(), is(false));
        }
      }
    }
  }

  /**
   * Check if both the {@link ResultSet} passed in has equivalent data. Checks the toString of the
   * underlying data returned.
   *
   * @param rs1 ResultSet1 to compare
   * @param rs2 ResultSet2 to compare
   * @throws SQLException Will be thrown if any of the Driver calls fail
   */
  private void checkResultSetEqual(ResultSet rs1, ResultSet rs2) throws SQLException {
    int columns = rs1.getMetaData().getColumnCount();
    assertEquals(
        columns,
        rs2.getMetaData().getColumnCount(),
        "Resultsets do not match in the number of columns returned");

    while (rs1.next() && rs2.next()) {
      for (int columnIndex = 1; columnIndex <= columns; columnIndex++) {
        final Object res1 = rs1.getObject(columnIndex);
        final Object res2 = rs2.getObject(columnIndex);

        assertEquals(
            res1,
            res2,
            String.format("%s and %s are not equal values at column %d", res1, res2, columnIndex));
      }

      assertEquals(
          rs1.isLast(), rs2.isLast(), "Number of records returned by the results does not match");
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPreparedStatementWithSkipParsing(String queryResultFormat) throws Exception {
    try (Connection con = getConn(queryResultFormat)) {
      PreparedStatement stmt =
          con.unwrap(SnowflakeConnectionV1.class).prepareStatement("select 1", true);
      try (ResultSet rs = stmt.executeQuery()) {
        assertThat(rs.next(), is(true));
        assertThat(rs.getInt(1), is(1));
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPreparedStatementWithSkipParsingAndBinding(String queryResultFormat)
      throws Exception {
    try (Connection con = getConn(queryResultFormat);
        Statement statement = con.createStatement()) {
      statement.execute("create or replace table t(c1 int)");
      try {
        try (PreparedStatement stmt =
            con.unwrap(SnowflakeConnectionV1.class)
                .prepareStatement("insert into t(c1) values(?)", true)) {
          stmt.setInt(1, 123);
          int ret = stmt.executeUpdate();
          assertThat(ret, is(1));
        }
        try (PreparedStatement stmt =
                con.unwrap(SnowflakeConnectionV1.class).prepareStatement("select * from t", true);
            ResultSet rs = stmt.executeQuery()) {
          assertThat(rs.next(), is(true));
          assertThat(rs.getInt(1), is(123));
        }
      } finally {
        statement.execute("drop table if exists t");
      }
    }
  }

  /**
   * Test prepare mode for to_timestamp(?, 3), compiler right now can not handle this properly. A
   * workaround is added. More specifically, ErrorCode returned for this statement is caught in
   * SnowflakePreparedStatementV1 so that execution can continue
   */
  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testSnow44393(String queryResultFormat) throws Exception {
    try (Connection con = getConn(queryResultFormat)) {
      assertFalse(
          con.createStatement()
              .execute("alter session set timestamp_ntz_output_format='YYYY-MM-DD HH24:MI:SS'"));
      try (PreparedStatement stmt = con.prepareStatement("select to_timestamp_ntz(?, 3)")) {
        stmt.setBigDecimal(1, new BigDecimal("1261440000000"));
        try (ResultSet resultSet = stmt.executeQuery()) {
          assertTrue(resultSet.next());

          String res = resultSet.getString(1);
          assertThat(res, is("2009-12-22 00:00:00"));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  @DontRunOnGithubActions
  public void testAddBatchNumericNullFloatMixed(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat)) {
      for (int threshold = 0; threshold < 2; ++threshold) {
        connection
            .createStatement()
            .execute(
                "ALTER SESSION SET CLIENT_STAGE_ARRAY_BINDING_THRESHOLD = "
                    + threshold); // enable stage bind
        String sql = "insert into TEST_PREPST(col, colB) values(?,?)";

        // Null NUMERIC followed by FLOAT.
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
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
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
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
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
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
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
          stmt.setInt(1, 1);
          stmt.setNull(2, java.sql.Types.NUMERIC);
          stmt.addBatch();
          stmt.setInt(1, 2);
          stmt.setObject(2, 4.0f);
          stmt.addBatch();
          stmt.setInt(1, 1);
          stmt.setObject(2, "test1");
          SnowflakeSQLException ex = assertThrows(SnowflakeSQLException.class, stmt::addBatch);
          assertThat(
              "Error code is wrong",
              ex.getErrorCode(),
              equalTo(ErrorCode.ARRAY_BIND_MIXED_TYPES_NOT_SUPPORTED.getMessageCode()));
          assertThat("Location", ex.getMessage(), containsString("Column: 2, Row: 3"));
        }
      }
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testInvalidUsageOfApi(String queryResultFormat) throws Exception {
    try (Connection connection = getConn(queryResultFormat);
        PreparedStatement preparedStatement = connection.prepareStatement("select 1")) {
      final int expectedCode =
          ErrorCode.UNSUPPORTED_STATEMENT_TYPE_IN_EXECUTION_API.getMessageCode();

      assertException(
          new RunnableWithSQLException() {
            @Override
            public void run() throws SQLException {
              preparedStatement.executeUpdate("select 1");
            }
          },
          expectedCode);

      assertException(
          new RunnableWithSQLException() {
            @Override
            public void run() throws SQLException {
              preparedStatement.execute("select 1");
            }
          },
          expectedCode);

      assertException(
          new RunnableWithSQLException() {
            @Override
            public void run() throws SQLException {
              preparedStatement.addBatch("select 1");
            }
          },
          expectedCode);
    }
  }

  private void assertException(RunnableWithSQLException runnable, int expectedCode) {
    SQLException e = assertThrows(SQLException.class, runnable::run);
    assertThat(e.getErrorCode(), is(expectedCode));
  }

  private interface RunnableWithSQLException {
    void run() throws SQLException;
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testCreatePreparedStatementWithParameters(String queryResultFormat) throws Throwable {
    try (Connection connection = getConn(queryResultFormat)) {
      connection.prepareStatement(
          "select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      SQLException ex =
          assertThrows(
              SQLException.class,
              () ->
                  connection.prepareStatement(
                      "select 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));
      assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
      connection.prepareStatement(
          "select 1",
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT);
      ex =
          assertThrows(
              SQLException.class,
              () ->
                  connection.prepareStatement(
                      "select 1",
                      ResultSet.TYPE_FORWARD_ONLY,
                      ResultSet.CONCUR_READ_ONLY,
                      ResultSet.HOLD_CURSORS_OVER_COMMIT));
      assertEquals((int) ErrorCode.FEATURE_UNSUPPORTED.getMessageCode(), ex.getErrorCode());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(SimpleResultFormatProvider.class)
  public void testPrepareAndGetMeta(String queryResultFormat) throws SQLException {
    try (Connection con = getConn(queryResultFormat)) {
      try (PreparedStatement prepStatement = con.prepareStatement("select 1 where 1 > ?")) {
        ResultSetMetaData meta = prepStatement.getMetaData();
        assertThat(meta.getColumnCount(), is(1));
      }
      try (PreparedStatement prepStatement = con.prepareStatement("select 1 where 1 > ?")) {
        ParameterMetaData parameterMetaData = prepStatement.getParameterMetaData();
        assertThat(parameterMetaData.getParameterCount(), is(1));
      }
    }
  }
}
