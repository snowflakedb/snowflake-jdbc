package net.snowflake.client.jdbc.telemetryOOB;


import jdk.nashorn.internal.ir.annotations.Ignore;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.BaseJDBCTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * Standalone test cases for the out of band telemetry service
 */
public class TelemetryServiceIT extends BaseJDBCTest
{
  private boolean defaultState;
  private String defaultDeployment;

  @Before
  public void setUp()
  {
    TelemetryService service = TelemetryService.getInstance();
//    try
//    {
//      service.updateContext(getConnectionParameters());
//    }
//    catch (SQLException e)
//    {
//      e.printStackTrace();
//    }
    defaultState = service.isEnabled();
    defaultDeployment = service.getServerDeploymentName();
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
    try{
      int a = 10/0;
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
      if(service.size()==0){
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
  public void testCreateMetric()
  {
    // this metric will be delivered to wavefront
    TelemetryService service = TelemetryService.getInstance();
    TelemetryEvent.MetricBuilder mBuilder = new TelemetryEvent.MetricBuilder();
    TelemetryEvent metric = mBuilder
        .withName("ExampleMetricCount")
        .withValue(1)
        .withTag("domain", "test")
        .build();
    assertThat("check metric name", metric.get("Name").equals("ExampleMetricCount"));
    assertThat("check metric value", (int)metric.get("Value") == 1);
    JSONArray tags = (JSONArray) metric.get("Tags");
    for (int i=0;i<tags.size();i++)
    {
      JSONObject tag = (JSONObject) tags.get(i);
      if (tag.getAsString("Name").compareTo("stage") == 0)
      {
        assertThat("check stage name",
            tag.getAsString("Value").compareTo(defaultDeployment) == 0);
      }
    }
    service.add(metric);
    service.flush();
  }
}
