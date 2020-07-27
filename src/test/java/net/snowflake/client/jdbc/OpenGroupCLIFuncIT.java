/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
 */
package net.snowflake.client.jdbc;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import net.snowflake.client.AbstractDriverIT;
import net.snowflake.client.category.TestCategoryOthers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Test OpenGroup CLI */
@Category(TestCategoryOthers.class)
public class OpenGroupCLIFuncIT extends BaseJDBCTest {
  private Connection connection = null;
  private Statement statement = null;
  private ResultSet resultSet = null;

  public static Connection getConnection() throws SQLException {
    Connection connection = AbstractDriverIT.getConnection();

    Statement statement = connection.createStatement();
    statement.execute(
        "alter session set "
            + "TIMEZONE='America/Los_Angeles',"
            + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
            + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
            + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");
    statement.close();
    return connection;
  }

  @Test
  public void testNumericFunctions() throws SQLException {
    connection = getConnection();
    testFunction(connection, "select {fn ABS(-1)}", "1");
    testFunction(connection, "select {fn ACOS(0.5)}", "1.0471975511965979");
    testFunction(connection, "select {fn ASIN(0.5)}", "0.5235987755982989");
    testFunction(connection, "select {fn CEILING(1.3)}", "2");
    testFunction(connection, "select {fn COS(1.3)}", "0.26749882862458735");
    testFunction(connection, "select {fn COT(1.3)}", "0.27761564654112514");
    testFunction(connection, "select {fn DEGREES(1.047197551)}", "59.99999998873578");
    testFunction(connection, "select {fn EXP(2)}", "7.38905609893065");
    testFunction(connection, "select {fn FLOOR(1.2)}", "1");
    testFunction(connection, "select {fn LOG(1.2)}", "0.1823215567939546");
    // LOG10 is not supported
    // testFunction(connection, "select {fn LOG10(1.2)}", "1");
    testFunction(connection, "select {fn MOD(3, 2)}", "1");
    testFunction(connection, "select {fn PI()}", "3.141592653589793");
    testFunction(connection, "select {fn POWER(3, 2)}", "9.0");
    testFunction(connection, "select {fn RADIANS(1.2)}", "0.020943951023931952");
    testFunction(connection, "select {fn RAND(2)}", "-1778191858535396788");
    testFunction(connection, "select {fn ROUND(2.234456, 4)}", "2.2345");
    testFunction(connection, "select {fn SIGN(-10)}", "-1");
    testFunction(connection, "select {fn SQRT(9)}", "3.0");
    testFunction(connection, "select {fn TAN(9)}", "-0.45231565944180985");
    testFunction(connection, "select {fn TRUNCATE(2.234456, 4)}", "2.2344");
    connection.close();
  }

  @Test
  public void testStringFunction() throws SQLException {
    connection = getConnection();
    testFunction(connection, "select {fn ASCII('snowflake')}", "115");
    testFunction(connection, "select {fn CHAR(115)}", "s");
    testFunction(connection, "select {fn CONCAT('snow', 'flake')}", "snowflake");
    // DIFFERENCE is not supported
    // testFunction(connection, "select {fn DIFFERENCE('snow', 'flake')}", "snowflake");
    testFunction(connection, "select {fn INSERT('snowflake', 2, 3, 'insert')}", "sinsertflake");
    testFunction(connection, "select {fn LCASE('SNOWflake')}", "snowflake");
    testFunction(connection, "select {fn LEFT('snowflake', 4)}", "snow");
    testFunction(connection, "select {fn LENGTH('  snowflake  ')}", "11");
    testFunction(connection, "select {fn LOCATE('str', 'strstrstr', 2)}", "4");
    testFunction(connection, "select {fn LTRIM('  snowflake  ')}", "snowflake  ");
    testFunction(connection, "select {fn REPEAT('snow', 3)}", "snowsnowsnow");
    testFunction(connection, "select {fn REPLACE('snowssnowsn', 'sn', 'aa')}", "aaowsaaowaa");
    testFunction(connection, "select {fn RIGHT('snowflake', 5)}", "flake");
    testFunction(connection, "select {fn RTRIM('  snowflake  ')}", "  snowflake");
    // SOUNDEX is not supported
    // testFunction(connection, "select {fn SOUNDEX('snowflake')}", "  snowflake");
    testFunction(connection, "select {fn SPACE(4)}", "    ");
    testFunction(connection, "select {fn SUBSTRING('snowflake', 2, 3)}", "now");
    testFunction(connection, "select {fn UCASE('snowflake')}", "SNOWFLAKE");
    connection.close();
  }

  @Test
  public void testDateTimeFunction() throws SQLException {
    connection = getConnection();
    // testFunction(connection, "select {fn CURDATE()}","");
    // testFunction(connection, "select {fn CURTIME()}","");
    testFunction(connection, "select {fn DAYNAME('2016-5-25')}", "Wed");
    testFunction(connection, "select {fn DAYOFMONTH(to_date('2016-5-25'))}", "25");
    testFunction(connection, "select {fn DAYOFWEEK(to_date('2016-5-25'))}", "3");
    testFunction(connection, "select {fn DAYOFYEAR(to_date('2016-5-25'))}", "146");
    testFunction(connection, "select {fn HOUR(to_timestamp('2016-5-25 12:34:56.789789'))}", "12");
    testFunction(connection, "select {fn MINUTE(to_timestamp('2016-5-25 12:34:56.789789'))}", "34");
    testFunction(connection, "select {fn MONTH(to_date('2016-5-25'))}", "5");
    testFunction(connection, "select {fn MONTHNAME(to_date('2016-5-25'))}", "May");
    // testFunction(connection, "select {fn NOW()}", "May");
    testFunction(connection, "select {fn QUARTER(to_date('2016-5-25'))}", "2");
    testFunction(connection, "select {fn SECOND(to_timestamp('2016-5-25 12:34:56.789789'))}", "56");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_FRAC_SECOND, 1000, "
            + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Wed, 25 May 2016 12:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_SECOND, 1, "
            + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Wed, 25 May 2016 12:34:57 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_MINUTE, 1, "
            + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Wed, 25 May 2016 12:35:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_HOUR, 1, " + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Wed, 25 May 2016 13:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_DAY, 1, " + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Thu, 26 May 2016 12:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_MONTH, 1, "
            + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Sat, 25 Jun 2016 12:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_QUARTER, 1, "
            + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Thu, 25 Aug 2016 12:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPADD(SQL_TSI_YEAR, 1, " + "to_timestamp('2016-5-25 12:34:56.789789'))}",
        "Thu, 25 May 2017 12:34:56 -0700");
    testFunction(
        connection,
        "select {fn TIMESTAMPDIFF(SQL_TSI_SECOND, "
            + "to_timestamp('2016-5-25 12:34:56.789789'), to_timestamp('2016-5-25 12:34:57.789789'))}",
        "1");
    testFunction(connection, "select {fn WEEK(to_timestamp('2016-5-25 12:34:56.789789'))}", "21");
    testFunction(connection, "select {fn YEAR(to_timestamp('2016-5-25 12:34:56.789789'))}", "2016");
    connection.close();
  }

  @Test
  public void testSystemFunctions() throws SQLException {
    connection = getConnection();
    testFunction(connection, "select {fn DATABASE()}", connection.getCatalog());
    testFunction(connection, "select {fn IFNULL(NULL, 1)}", "1");
    testFunction(
        connection, "select {fn USER()}", System.getenv("SNOWFLAKE_TEST_USER").toUpperCase());
    connection.close();
  }

  private void testFunction(Connection connection, String sql, String expected)
      throws SQLException {
    statement = connection.createStatement();
    resultSet = statement.executeQuery(sql);
    assertTrue(resultSet.next());
    assertEquals(expected, resultSet.getString(1));
    statement.close();
  }
}
