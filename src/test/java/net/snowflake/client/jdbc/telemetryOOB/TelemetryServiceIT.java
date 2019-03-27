package net.snowflake.client.jdbc.telemetryOOB;


import net.snowflake.client.jdbc.BaseJDBCTest;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Standalone test cases for the out of band telemetry service
 */
public class TelemetryServiceIT extends BaseJDBCTest
{
  private boolean defaultState;

  @Before
  public void setUp()
  {
    TelemetryService service = TelemetryService.getInstance();
    service.updateContext(getConnectionParameters());
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
      service.add(log);

      // example for an exception metric
      // this metric will be delivered to snowflake and wavefront
      TelemetryEvent.MetricBuilder mBuilder = new TelemetryEvent.MetricBuilder();
      TelemetryEvent metric = mBuilder
          .withException(ex)
          .withTag("domain", "test")
          .build();
      System.out.println(metric);
      service.add(metric);

      service.flush();
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
    service.add(log);
    service.flush();
    // wait for at most 30 seconds until the queue is cleaned
    int i = 6;
    while (i-- > 0)
    {
      TimeUnit.SECONDS.sleep(5);
      if (service.size() == 0)
      {
        break;
      }
    }
    assertThat("WrongServerURL do not block.", service.size() == 0);
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
    assertThat("check log value", log.get("Value").equals("This is an example log"));
    service.add(log);
    service.flush();
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
    service.add(log);
    String marked = service.exportQueueToString();

    assertThat("marked aws_key_id", !marked.contains("xxdsdfsafds"));
    assertThat("marked aws_secret_key", !marked.contains("safas+asfsad+safasf"));
    service.flush();
  }

  @Ignore
  @Test
  public void stressTestCreateLog()
  {
    // this log will be delivered to snowflake
    TelemetryService service = TelemetryService.getInstance();
    // send one http request for each event
    service.setBatchSize(1);
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
        service.add(log);
      }
    }
    service.resetBatchSize();
    service.flush();
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
    service.add(log);
    service.flush();
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
        .withUrgent(true)
        .build();
    assertThat("check log value", log.get("Value").equals("This is an example urgent log"));
    service.add(log);
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
            .withUrgent(true)
            .withValue("This is an example urgent log for stress test " + sent)
            .build();
        System.out.println("stress test: " + sent++ + " sent.");
        service.add(log);
      }
    }
    service.flush();
    sw.stop();
  }
}
