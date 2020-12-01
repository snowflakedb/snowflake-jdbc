package net.snowflake.client.jdbc;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.node.ObjectNode;
import net.minidev.json.JSONObject;
import org.junit.Test;

public class SqlFeatureNotSupportedTelemetryTest {

  String queryId = "test-query-idfake";
  String SQLState = "00000";
  int vendorCode = 27;
  String driverVersion = SnowflakeDriver.implementVersion;

  String comparison =
      "{\"type\":\"client_sql_exception\",\"DriverType\":\"JDBC\",\"DriverVersion\":\""
          + driverVersion
          + "\","
          + "\"QueryID\":\""
          + queryId
          + "\",\"SQLState\":\""
          + SQLState
          + "\",\"ErrorNumber\":"
          + vendorCode
          + "}";

  /** Test that creating in-band objectNode looks as expected */
  @Test
  public void testCreateIBValue() {
    ObjectNode ibValue = SnowflakeSQLLoggedException.createIBValue(queryId, SQLState, vendorCode);
    assertEquals(comparison, ibValue.toString());
  }

  /** Test that creating out-of-band JSONObject contains all attributes it needs */
  @Test
  public void testCreateOOBValue() {
    JSONObject oobValue = SnowflakeSQLLoggedException.createOOBValue(queryId, SQLState, vendorCode);
    assertEquals("client_sql_exception", oobValue.get("type").toString());
    assertEquals("JDBC", oobValue.get("DriverType").toString());
    assertEquals(driverVersion, oobValue.get("DriverVersion").toString());
    assertEquals(queryId, oobValue.get("QueryID").toString());
    assertEquals(SQLState, oobValue.get("SQLState").toString());
    assertEquals(vendorCode, oobValue.get("ErrorNumber"));
  }

  @Test
  public void testMaskStacktrace() {
    // Unmasked stacktrace containing reason for failure after the exception type
    String snowflakeSQLStacktrace =
        "net.snowflake.client.jdbc.SnowflakeSQLLoggedException: This is a test exception.\n"
            + "\tat net.snowflake.client.jdbc.telemetryOOB.TelemetryServiceIT.generateDummyException(TelemetryServiceIT.java:211)\n";
    // Masked stacktrace with reason removed
    String maskedSnowflakeSQLStacktrace =
        "net.snowflake.client.jdbc.SnowflakeSQLLoggedException\n"
            + "\tat net.snowflake.client.jdbc.telemetryOOB.TelemetryServiceIT.generateDummyException(TelemetryServiceIT.java:211)\n";

    // Sometimes reason can be multiple lines
    String multipleLineReasonMessage =
        "net.snowflake.client.jdbc.SnowflakeSQLException: Error parsing JSON: {\"dsadas\n"
            + "adsa\":12311}\n"
            + "  File 'VvCSoHWHrB/0.CSV.gz', line 1, character 0\n"
            + "  Row 1, column \"SPARK_TEST_TABLE_8417843441957284451\"[\"VAR\":1]\n"
            + "  If you would like to continue loading when an error is encountered, use other values such as "
            + "'SKIP_FILE' or 'CONTINUE' for the ON_ERROR option. For more information on loading options, please "
            + "run 'info loading_data' in a SQL client.\n"
            + "\tat net.snowflake.client.jdbc.SnowflakeUtil.checkErrorAndThrowExceptionSub(SnowflakeUtil.java:124)\n";

    String maskedMultipleLineReasonMessage =
        "net.snowflake.client.jdbc.SnowflakeSQLException\n"
            + "\tat net.snowflake.client.jdbc.SnowflakeUtil.checkErrorAndThrowExceptionSub(SnowflakeUtil.java:124)\n";

    assertEquals(
        maskedSnowflakeSQLStacktrace,
        SnowflakeSQLLoggedException.maskStacktrace(snowflakeSQLStacktrace));

    // Unmasked stacktrace for SQLFeatureNotSupportedException. Contains reason as well
    String featureNotSupportedStacktrace =
        "net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException: Not supported!\n"
            + "\tat net.snowflake.client.jdbc.SnowflakeStatementV1.execute(SnowflakeStatementV1.java:344)\n";

    // Masked stacktrace
    String maskedFeatureNotSupportedStacktrace =
        "net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException\n"
            + "\tat net.snowflake.client.jdbc.SnowflakeStatementV1.execute(SnowflakeStatementV1.java:344)\n";

    assertEquals(
        maskedFeatureNotSupportedStacktrace,
        SnowflakeSQLLoggedException.maskStacktrace(featureNotSupportedStacktrace));

    assertEquals(
        maskedMultipleLineReasonMessage,
        SnowflakeSQLLoggedException.maskStacktrace(multipleLineReasonMessage));
  }
}
