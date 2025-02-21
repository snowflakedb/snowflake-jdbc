package net.snowflake.client.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.TimeZone;
import net.snowflake.client.providers.ProvidersUtil;
import net.snowflake.client.providers.ScaleProvider;
import net.snowflake.client.providers.SimpleResultFormatProvider;
import net.snowflake.client.providers.SnowflakeArgumentsProvider;
import net.snowflake.client.providers.TimezoneProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;

abstract class ResultSetArrowForce0MultiTimeZone extends BaseJDBCTest {
  protected static class DataProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return ProvidersUtil.cartesianProduct(
          context, new SimpleResultFormatProvider(), new TimezoneProvider(3));
    }
  }

  protected static class DataWithScaleProvider extends SnowflakeArgumentsProvider {
    @Override
    protected List<Arguments> rawArguments(ExtensionContext context) {
      return ProvidersUtil.cartesianProduct(context, new DataProvider(), new ScaleProvider());
    }
  }

  private static TimeZone origTz;

  @BeforeAll
  public static void setUp() {
    origTz = TimeZone.getDefault();
  }

  @AfterAll
  public static void tearDown() {
    TimeZone.setDefault(origTz);
  }

  protected static void setTimezone(String tz) {
    TimeZone.setDefault(TimeZone.getTimeZone(tz));
  }

  Connection init(String table, String column, String values, String queryResultFormat)
      throws SQLException {
    Connection con = BaseJDBCTest.getConnection();

    try (Statement statement = con.createStatement()) {
      statement.execute(
          "alter session set "
              + "TIMEZONE='America/Los_Angeles',"
              + "TIMESTAMP_TYPE_MAPPING='TIMESTAMP_LTZ',"
              + "TIMESTAMP_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_TZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_LTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM',"
              + "TIMESTAMP_NTZ_OUTPUT_FORMAT='DY, DD MON YYYY HH24:MI:SS TZHTZM'");

      statement.execute(
          "alter session set jdbc_query_result_format" + " = '" + queryResultFormat + "'");
      statement.execute("create or replace table " + table + " " + column);
      statement.execute("insert into " + table + " values " + values);
    }
    return con;
  }

  protected void finish(String table, Connection con) throws SQLException {
    con.createStatement().execute("drop table " + table);
    con.close();
    System.clearProperty("user.timezone");
  }
}
