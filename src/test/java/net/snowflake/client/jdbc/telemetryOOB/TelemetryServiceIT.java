package net.snowflake.client.jdbc.telemetryOOB;


import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.category.TestCategoryCore;
import net.snowflake.client.jdbc.SnowflakeSQLLoggedException;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Standalone test cases for the out of band telemetry service
 */
@Category(TestCategoryCore.class)
public class TelemetryServiceIT extends BaseJDBCTest
{
  private boolean defaultState;

  @Before
  public void setUp()
  {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContextForIT(getConnectionParameters());
    defaultState = service.isEnabled();
    service.enable();
  }

  @After
  public void tearDown() throws InterruptedException
  {
    // wait 5 seconds while the service is flushing
    TimeUnit.SECONDS.sleep(5);
    TelemetryService service = TelemetryService.getInstance();
    if (defaultState)
    {
      service.enable();
    }
    else
    {
      service.disable();
    }
  }

  @SuppressWarnings("divzero")
  @Ignore
  @Test
  public void testCreateException()
  {
    TelemetryService service = TelemetryService.getInstance();
    try
    {
      int a = 10 / 0;
    }
    catch (Exception ex)
    {
      // example for an exception log
      // this log will be delivered to snowflake
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
      TelemetryEvent log = logBuilder
          .withException(ex)
          .build();
      System.out.println(log);
      service.report(log);

      // example for an exception metric
      // this metric will be delivered to snowflake and wavefront
      TelemetryEvent.MetricBuilder mBuilder =
          new TelemetryEvent.MetricBuilder();
      TelemetryEvent metric = mBuilder
          .withException(ex)
          .withTag("domain", "test")
          .build();
      System.out.println(metric);
      service.report(metric);
    }
  }

  /**
   * test wrong server url.
   */
  @Ignore
  @Test
  public void testWrongServerURL() throws InterruptedException
  {
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log = logBuilder
        .withName("ExampleLog")
        .withValue("This is an example log")
        .build();
    int count = service.getEventCount();
    service.report(log);
    // wait for at most 30 seconds
    int i = 6;
    while (i-- > 0)
    {
      TimeUnit.SECONDS.sleep(5);
      if (service.getEventCount() > count)
      {
        break;
      }
    }
    assertThat("WrongServerURL do not block.", service.getEventCount() > count);
  }

  @Ignore
  @Test
  public void testCreateLog()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log = logBuilder
        .withName("ExampleLog")
        .withValue("This is an example log")
        .build();
    assertThat("check log value",
               log.get("Value").equals("This is an example log"));
    service.report(log);
  }

  @Ignore
  @Test
  public void testCreateLogWithAWSSecret()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log = logBuilder
        .withName("ExampleLog")
        .withValue("This is an example log: credentials=(\n" +
                   "  aws_key_id='xxdsdfsafds'\n" +
                   "  aws_secret_key='safas+asfsad+safasf')\n")
        .build();
    String marked = service.exportQueueToString(log);

    assertThat("marked aws_key_id", !marked.contains("xxdsdfsafds"));
    assertThat("marked aws_secret_key",
               !marked.contains("safas+asfsad+safasf"));
    service.report(log);
  }

  @Ignore
  @Test
  public void stressTestCreateLog()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    // send one http request for each event
    StopWatch sw = new StopWatch();
    sw.start();
    int rate = 50;
    int sent = 0;
    int duration = 60;
    while (sw.getTime() < duration * 1000)
    {
      int toSend = (int) (sw.getTime() / 1000) * rate - sent;
      for (int i = 0; i < toSend; i++)
      {
        TelemetryEvent log = new TelemetryEvent.LogBuilder()
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
  public void testCreateLogInBlackList()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log = logBuilder
        .withName("unknown")
        .withValue("This is a log in blacklist")
        .build();
    service.report(log);
  }

  @Ignore
  @Test
  public void testCreateUrgentEvent()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
    TelemetryEvent log = logBuilder
        .withName("UrgentLog")
        .withValue("This is an example urgent log")
        .build();
    assertThat("check log value",
               log.get("Value").equals("This is an example urgent log"));
    service.report(log);
  }

  @Ignore
  @Test
  public void stressTestCreateUrgentEvent()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    // send one http request for each event
    StopWatch sw = new StopWatch();
    sw.start();
    int rate = 1;
    int sent = 0;
    int duration = 5;
    while (sw.getTime() < duration * 1000)
    {
      int toSend = (int) (sw.getTime() / 1000) * rate - sent;
      for (int i = 0; i < toSend; i++)
      {
        TelemetryEvent log = new TelemetryEvent.LogBuilder()
            .withName("StressUrgentTestLog")
            .withValue("This is an example urgent log for stress test " + sent)
            .build();
        System.out.println("stress test: " + sent++ + " sent.");
        service.report(log);
      }
    }
    sw.stop();
  }

  /**
   * Manual test case for checking telemetry message for SnowflakeSQLExceptions. Connect to dev or prod account and
   * query client_telemetry table in s3testaccount for results.
   * @throws SQLException
   */
  @Ignore
  @Test
  public void testSnowflakeSQLLoggedExceptionTelemetry() throws SQLException
  {
    // make a connection to initialize telemetry instance
    Connection con = getConnection();
    try {
      generateDummyException(5);
    }
    catch (SnowflakeSQLLoggedException e)
    {
      e.printStackTrace();
    }
    /* Telemetry data should look like this when collected:
    {
      "Created_On": "2020-07-01 21:48:22",
      "Enqueued_On": "2020-07-01 21:48:23",
      "Name": "Exception: This is a test exception.",
      "SchemaVersion": 1,
      "Staged_On": "2020-07-01 21:48:44",
      "Tags": {
        "UUID": "f5e8ba4e-4136-4a32-9b69-c4824971a460",
        "connectionString": "http://snowflake.dev.local:8080?ROLE=sysadmin&ACCOUNT=testaccount&PASSWORD=☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺☺",
        "ctx_account": "testaccount",
        "ctx_db": "testdb",
        "ctx_insecuremode": "false",
        "ctx_internal": "true",
        "ctx_role": "sysadmin",
        "ctx_schema": "testschema",
        "ctx_ssl": "off",
        "ctx_user": "snowman",
        "ctx_warehouse": "regress",
        "driver": "JDBC",
        "snowhouseSchema": "dev",
        "telemetryServerDeployment": "dev",
        "version": "3.12.8"
      },
      "Type": "Log",
      "UUID": "f5e8ba4e-4136-4a32-9b69-c4824971a460",
      "Value": {
        "Query ID": "01234567-1234-1234-1234-00001abcdefg",
        "SQLState": "02000",
        "Stacktrace": "net.snowflake.client.jdbc.SnowflakeSQLLoggedException: This is a test exception.\n\tat net.snowflake.client.jdbc.telemetryOOB.TelemetryServiceIT.generateDummyException(TelemetryServiceIT.java:246)\n\tat net.snowflake.client.jdbc.telemetryOOB.TelemetryServiceIT.testSnowflakeSQLLoggedExceptionTelemetry(TelemetryServiceIT.java:262)\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)\n\tat java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\n\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\n\tat java.base/java.lang.reflect.Method.invoke(Method.java:567)\n\tat org.junit.runners.model.FrameworkMethod$1.runReflectiveCall(FrameworkMethod.java:50)\n\tat org.junit.internal.runners.model.ReflectiveCallable.run(ReflectiveCallable.java:12)\n\tat org.junit.runners.model.FrameworkMethod.invokeExplosively(FrameworkMethod.java:47)\n\tat org.junit.internal.runners.statements.InvokeMethod.evaluate(InvokeMethod.java:17)\n\tat org.junit.internal.runners.statements.RunBefores.evaluate(RunBefores.java:26)\n\tat org.junit.internal.runners.statements.RunAfters.evaluate(RunAfters.java:27)\n\tat org.junit.runners.ParentRunner.runLeaf(ParentRunner.java:325)\n\tat org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:78)\n\tat org.junit.runners.BlockJUnit4ClassRunner.runChild(BlockJUnit4ClassRunner.java:57)\n\tat org.junit.runners.ParentRunner$3.run(ParentRunner.java:290)\n\tat org.junit.runners.ParentRunner$1.schedule(ParentRunner.java:71)\n\tat org.junit.runners.ParentRunner.runChildren(ParentRunner.java:288)\n\tat org.junit.runners.ParentRunner.access$000(ParentRunner.java:58)\n\tat org.junit.runners.ParentRunner$2.evaluate(ParentRunner.java:268)\n\tat org.junit.runners.ParentRunner.run(ParentRunner.java:363)\n\tat org.junit.runner.JUnitCore.run(JUnitCore.java:137)\n\tat com.intellij.junit4.JUnit4IdeaTestRunner.startRunnerWithArgs(JUnit4IdeaTestRunner.java:68)\n\tat com.intellij.rt.junit.IdeaTestRunner$Repeater.startRunnerWithArgs(IdeaTestRunner.java:33)\n\tat com.intellij.rt.junit.JUnitStarter.prepareStreamsAndStart(JUnitStarter.java:230)\n\tat com.intellij.rt.junit.JUnitStarter.main(JUnitStarter.java:58)\n",
        "Vendor Code": 0
      }
    }
     */
  }
}
