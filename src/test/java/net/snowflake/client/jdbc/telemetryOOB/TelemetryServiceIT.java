package net.snowflake.client.jdbc.telemetryOOB;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.core.SFSession;
import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeLoggedFeatureNotSupportedException;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import net.snowflake.common.core.SqlState;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Standalone test cases for the out of band telemetry service */
@Category(TestCategoryCore.class)
public class TelemetryServiceIT extends BaseJDBCTest {
  private static final int WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS = 5000;
  private boolean defaultState;

  @Before
  public void setUp() {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException {
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);
    TelemetryService service = TelemetryService.getInstance();
    if (defaultState) {
      service.enable();
    } else {
      service.disable();
    }
  }

  @SuppressWarnings("divzero")
  @Ignore
  @Test
  public void testCreateException() {
    TelemetryService service = TelemetryService.getInstance();
    try {
      int a = 10 / 0;
    } catch (Exception ex) {
      // example for an exception log
      // this log will be delivered to snowflake
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
      TelemetryEvent log = logBuilder.withException(ex).build();
      System.out.println(log);
      service.report(log);

      // example for an exception metric
      // this metric will be delivered to snowflake and wavefront
      TelemetryEvent.MetricBuilder mBuilder = new TelemetryEvent.MetricBuilder();
      TelemetryEvent metric = mBuilder.withException(ex).withTag("domain", "test").build();
      System.out.println(metric);
      service.report(metric);
    }
  }

  /** test wrong server url. */
  @Ignore
  @Test
  public void testWrongServerURL() throws InterruptedException {
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log =
        logBuilder.withName("ExampleLog").withValue("This is an example log").build();
    int count = service.getEventCount();
    service.report(log);
    // wait for at most 30 seconds
    int i = 6;
    while (i-- > 0) {
      TimeUnit.SECONDS.sleep(5);
      if (service.getEventCount() > count) {
        break;
      }
    }
    assertThat("WrongServerURL do not block.", service.getEventCount() > count);
  }

  @Ignore
  @Test
  public void testCreateLog() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log =
        logBuilder.withName("ExampleLog").withValue("This is an example log").build();
    assertThat("check log value", log.get("Value").equals("This is an example log"));
    service.report(log);
  }

  @Ignore
  @Test
  public void testCreateLogWithAWSSecret() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log =
        logBuilder
            .withName("ExampleLog")
            .withValue(
                "This is an example log: credentials=(\n"
                    + "  aws_key_id='xxdsdfsafds'\n"
                    + "  aws_secret_key='safas+asfsad+safasf')\n")
            .build();
    String marked = service.exportQueueToString(log);

    assertThat("marked aws_key_id", !marked.contains("xxdsdfsafds"));
    assertThat("marked aws_secret_key", !marked.contains("safas+asfsad+safasf"));
    service.report(log);
  }

  @Ignore
  @Test
  public void stressTestCreateLog() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    // send one http request for each event
    StopWatch sw = new StopWatch();
    sw.start();
    int rate = 50;
    int sent = 0;
    int duration = 60;
    while (sw.getTime() < duration * 1000) {
      int toSend = (int) (sw.getTime() / 1000) * rate - sent;
      for (int i = 0; i < toSend; i++) {
        TelemetryEvent log =
            new TelemetryEvent.LogBuilder()
                .withName("StressTestLog")
                .withValue("This is an example log for stress test " + sent)
                .build();
        System.out.println("stress test: " + sent++ + " sent.");
        service.report(log);
      }
    }
    sw.stop();
  }

  @Ignore
  @Test
  public void testCreateLogInBlackList() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log =
        logBuilder.withName("unknown").withValue("This is a log in blacklist").build();
    service.report(log);
  }

  @Ignore
  @Test
  public void testCreateUrgentEvent() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log =
        logBuilder.withName("UrgentLog").withValue("This is an example urgent log").build();
    assertThat("check log value", log.get("Value").equals("This is an example urgent log"));
    service.report(log);
  }

  @Ignore
  @Test
  public void stressTestCreateUrgentEvent() {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    // send one http request for each event
    StopWatch sw = new StopWatch();
    sw.start();
    int rate = 1;
    int sent = 0;
    int duration = 5;
    while (sw.getTime() < duration * 1000) {
      int toSend = (int) (sw.getTime() / 1000) * rate - sent;
      for (int i = 0; i < toSend; i++) {
        TelemetryEvent log =
            new TelemetryEvent.LogBuilder()
                .withName("StressUrgentTestLog")
                .withValue("This is an example urgent log for stress test " + sent)
                .build();
        System.out.println("stress test: " + sent++ + " sent.");
        service.report(log);
      }
    }
    sw.stop();
  }

  private void generateDummyException(int vendorCode, SFSession session)
      throws SnowflakeSQLLoggedException {
    String queryID = "01234567-1234-1234-1234-00001abcdefg";
    String reason = "This is a test exception.";
    String sqlState = SqlState.NO_DATA;
    throw new SnowflakeSQLLoggedException(session, reason, sqlState, vendorCode, queryID);
  }

  private int generateSQLFeatureNotSupportedException() throws SQLFeatureNotSupportedException {
    throw new SnowflakeLoggedFeatureNotSupportedException(null);
  }

  /**
   * Test case for checking telemetry message for SnowflakeSQLExceptions. Assert that telemetry OOB
   * endpoint is reached after a SnowflakeSQLLoggedException is thrown.
   *
   * @throws SQLException
   */
  @Test
  public void testSnowflakeSQLLoggedExceptionOOBTelemetry()
      throws SQLException, InterruptedException {
    // make a connection to initialize telemetry instance
    int fakeVendorCode = 27;
    try {
      generateDummyException(fakeVendorCode, null);
      fail();
    } catch (SnowflakeSQLLoggedException e) {
      // The error response has the same code as the the fakeErrorCode
      assertThat("Communication error", e.getErrorCode(), equalTo(fakeVendorCode));

      // since it returns normal response,
      // the telemetry does not create new event
      Thread.sleep(WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS);
      if (TelemetryService.getInstance().isDeploymentEnabled()) {
        assertThat(
            "Telemetry event has not been reported successfully. Error: "
                + TelemetryService.getInstance().getLastClientError(),
            TelemetryService.getInstance().getClientFailureCount(),
            equalTo(0));
      }
    }
  }

  /**
   * Test case for checking telemetry message for SqlFeatureNotSupportedExceptions. Assert that
   * telemetry OOB endpoint is reached after a SnowflakeSQLLoggedException is thrown.
   *
   * <p>After running test, check for result in client_telemetry_oob table with type
   * client_sql_exception.
   *
   * @throws SQLException
   */
  @Test
  public void testSQLFeatureNotSupportedOOBTelemetry() throws InterruptedException {
    // with null session, OOB telemetry will be thrown
    try {
      generateSQLFeatureNotSupportedException();
      fail("SqlFeatureNotSupportedException failed to throw.");
    } catch (SQLFeatureNotSupportedException e) {
      // since it returns normal response,
      // the telemetry does not create new event
      Thread.sleep(WAIT_FOR_TELEMETRY_REPORT_IN_MILLISECS);
      if (TelemetryService.getInstance().isDeploymentEnabled()) {
        assertThat(
            "Telemetry event has not been reported successfully. Error: "
                + TelemetryService.getInstance().getLastClientError(),
            TelemetryService.getInstance().getClientFailureCount(),
            equalTo(0));
      }
    }
  }

  /**
   * Test case for checking telemetry message for SnowflakeSQLExceptions. In-band telemetry should
   * be used.
   *
   * @throws SQLException
   */
  @Test
  public void testSnowflakeSQLLoggedExceptionIBTelemetry() throws SQLException {
    // make a connection to initialize telemetry instance
    try (Connection con = getConnection()) {
      int fakeErrorCode = 27;
      try {
        generateDummyException(
            fakeErrorCode, con.unwrap(SnowflakeConnectionV1.class).getSfSession());
        fail();
      } catch (SnowflakeSQLLoggedException e) {
        // The error response has the same code as the fakeErrorCode
        assertThat("Communication error", e.getErrorCode(), equalTo(fakeErrorCode));
      }
    }
  }

  /**
   * Test case for checking telemetry message for SnowflakeFeatureNotSupporteExceptions. In-band
   * telemetry should be used.
   *
   * <p>After running test, check for telemetry message in client_telemetry_v table.
   *
   * @throws SQLException
   */
  @Test(expected = SQLFeatureNotSupportedException.class)
  public void testSqlFeatureNotSupportedExceptionIBTelemetry() throws SQLException {
    // make a connection to initialize telemetry instance
    try (Connection con = getConnection()) {
      Statement statement = con.createStatement();
      // try to execute a statement that throws a SQLFeatureNotSupportedException
      statement.execute("select 1", new int[] {});
    }
  }
}
