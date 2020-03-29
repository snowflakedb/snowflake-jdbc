package net.snowflake.client.jdbc.telemetryOOB;


import net.snowflake.client.jdbc.BaseJDBCTest;
import net.snowflake.client.category.TestCategoryOthers;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Standalone test cases for the out of band telemetry service
 */
@Category(TestCategoryOthers.class)
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
}
