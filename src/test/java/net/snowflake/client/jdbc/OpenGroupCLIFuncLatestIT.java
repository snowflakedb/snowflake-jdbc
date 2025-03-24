package net.snowflake.client.jdbc;

import static net.snowflake.client.jdbc.OpenGroupCLIFuncIT.testFunction;

import java.sql.Connection;
import java.sql.SQLException;
import net.snowflake.client.category.TestTags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Open Group CLI function integration tests for the latest JDBC driver. This doesn't work for the
 * oldest supported driver. Revisit this tests whenever bumping up the oldest supported driver to
 * examine if the tests still are not applicable. If it is applicable, move tests to
 * OpenGroupCLIFuncIT so that both the latest and oldest supported driver run the tests.
 */
@Tag(TestTags.OTHERS)
public class OpenGroupCLIFuncLatestIT extends BaseJDBCTest {
  /**
   * Numeric function tests
   *
   * @throws SQLException arises if any exception occurs
   */
  @Test
  public void testNumericFunctions() throws SQLException {
    try (Connection connection = getConnection()) {
      testFunction(connection, "select {fn ABS(-1)}", "1");
      // This doesn't work for the old driver which doesn't support Arrow format yet where the arrow
      // float data type reserves higher precision.
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
    }
  }
}
