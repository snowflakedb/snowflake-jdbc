package net.snowflake.client.jdbc.telemetryOOB;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 * <p>
 * Out of Band Telemetry Service
 * This is a thread safe singleton queue containing telemetry messages
 */
public class TelemetryService
{
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(TelemetryService.class);

  private static ThreadLocal<TelemetryService> _threadLocal =
      new ThreadLocal<TelemetryService>()
      {
        @Override
        protected TelemetryService initialValue()
        {
          return new TelemetryService();
        }
      };

  // Global parameters:
  /**
   * control which deployments are enabled:
   * the service skips all events for the disabled deployments
   */
  private static HashSet<String> ENABLED_DEPLOYMENT
      = new HashSet<>(Arrays.asList(
      TELEMETRY_SERVER_DEPLOYMENT.DEV.name,
      TELEMETRY_SERVER_DEPLOYMENT.REG.name,
      TELEMETRY_SERVER_DEPLOYMENT.QA1.name,
      TELEMETRY_SERVER_DEPLOYMENT.PREPROD2.name
  ));

  /**
   * @return return thread local instance
   */
  public static TelemetryService getInstance()
  {
    return _threadLocal.get();
  }

  private TelemetryService()
  {
    try
    {
      SecurityManager sm = System.getSecurityManager();
      if (sm != null)
      {
        sm.checkPermission(new RuntimePermission("shutdownHooks"));
      }
      Runtime.getRuntime().addShutdownHook(new TelemetryUploader(this));
    }
    catch (SecurityException e)
    {
      logger.debug("Failed to add shutdown hook for telemetry service");
    }
  }

  // local parameters
  private LinkedList<TelemetryEvent> queue = new LinkedList<>();
  private int BATCH_SIZE = 100;
  private TELEMETRY_SERVER_DEPLOYMENT serverDeployment;

  /**
   * control enable/disable the whole service:
   * disabled service will skip added events and uploading to the server
   */
  private boolean enabled = false;

  public void enable()
  {
    enabled = true;
  }

  public void disable()
  {
    enabled = false;
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  /**
   * the context (e.g., connection properties) to be included in the telemetry events
   */
  private JSONObject context;

  public JSONObject getContext()
  {
    return context;
  }

  public void updateContext(Map<String, String> params)
  {
    Properties info = new Properties();
    for (String key : params.keySet())
    {
      Object val = params.get(key);
      if (val != null)
      {
        info.put(key, val);
      }
    }
    this.updateContext(params.get("uri"), info);
  }

  public void updateContext(final String url, final Properties info)
  {
    configureDeployment(url, info.getProperty("account"),
                        info.getProperty("port"));
    final Enumeration<?> names = info.propertyNames();
    context = new JSONObject();
    while (names.hasMoreElements())
    {
      String name = (String) names.nextElement();
      // remove sensitive properties
      if (name.compareTo("password") != 0 && name.compareTo("privateKey") != 0)
      {
        context.put(name, info.getProperty(name));
      }
    }
  }

  /**
   * configure telemetry deployment based on connection url and info
   * Note: it is not thread-safe while connecting to different deployments
   * simultaneously.
   *
   * @param url
   * @param account
   */
  private void configureDeployment(final String url, final String account,
                                   final String port)
  {
    // default value
    TELEMETRY_SERVER_DEPLOYMENT deployment = TELEMETRY_SERVER_DEPLOYMENT.PROD;
    if (url != null)
    {
      if (url.contains("reg") || url.contains("local"))
      {
        deployment = TELEMETRY_SERVER_DEPLOYMENT.REG;
        if ((port != null && port.compareTo("8080") == 0)
            || url.contains("8080"))
        {
          deployment = TELEMETRY_SERVER_DEPLOYMENT.DEV;
        }
      }
      else if (url.contains("qa1") ||
               (account != null && account.contains("qa1")))
      {
        deployment = TELEMETRY_SERVER_DEPLOYMENT.QA1;
      }
      else if (url.contains("preprod2"))
      {
        deployment = TELEMETRY_SERVER_DEPLOYMENT.PREPROD2;
      }
    }
    this.setDeployment(deployment);
  }

  /**
   * whether the telemetry service is enabled for current deployment
   */
  public boolean isDeploymentEnabled()
  {
    return ENABLED_DEPLOYMENT.contains(this.serverDeployment.name);
  }

  public TelemetryEvent getEvent(int i)
  {
    if (queue != null && queue.size() > i && i >= 0)
    {
      return queue.get(i);
    }
    return null;
  }

  private enum TELEMETRY_SERVER_URL
  {
    SFCTEST("https://lximwp8945.execute-api.us-west-2.amazonaws.com/sfctest/enqueue"),
    SFCDEV("https://lol6l3j52m.execute-api.us-west-2.amazonaws.com/sfcdev/enqueue"),
    US2("https://lol6l3j52m.execute-api.us-west-2.amazonaws.com/sfctest/enqueue"); // todo: change the url

    private final String url;

    TELEMETRY_SERVER_URL(String url)
    {
      this.url = url;
    }
  }

  public enum TELEMETRY_SERVER_DEPLOYMENT
  {
    DEV("dev", TELEMETRY_SERVER_URL.SFCTEST.url),
    REG("reg", TELEMETRY_SERVER_URL.SFCTEST.url),
    QA1("qa1", TELEMETRY_SERVER_URL.SFCDEV.url),
    PREPROD2("preprod2", TELEMETRY_SERVER_URL.SFCDEV.url),
    PROD("prod", TELEMETRY_SERVER_URL.US2.url),
    NONEXISTENT("nonexistent",
                "https://nonexist.execute-api.us-west-2.amazonaws.com/sfctest/enqueue");

    private final String name;
    private final String url;

    TELEMETRY_SERVER_DEPLOYMENT(String name, String url)
    {
      this.name = name;
      this.url = url;
    }

    public String getURL()
    {
      return url;
    }

    public String getName()
    {
      return name;
    }
  }

  public void setDeployment(TELEMETRY_SERVER_DEPLOYMENT deployment)
  {
    serverDeployment = deployment;
  }

  public String getServerDeploymentName()
  {
    return serverDeployment.name;
  }


  static class TelemetryUploader extends Thread
  {
    TelemetryService instance;

    public TelemetryUploader(TelemetryService _instance)
    {
      instance = _instance;
    }

    public void run()
    {
      if (!instance.enabled)
      {
        return;
      }
      if (instance.queue.isEmpty())
      {
        logger.debug("skip to run telemetry uploader for empty queue");
      }
      else
      {
        // flush the queue
        flushQueue();
        logger.debug("run telemetry uploader");
      }
    }

    private void flushQueue()
    {
      HttpResponse response = null;
      boolean success = true;
      try
      {
        if (!instance.isDeploymentEnabled())
        {
          // skip the disabled deployment
          logger.debug("skip the disabled deployment: "
                       + instance.serverDeployment.name);
          return;
        }
        HttpClient httpClient = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost(instance.serverDeployment.url);
        post.setEntity(new StringEntity(instance.logsToString(instance.queue)));
        post.setHeader("Content-type", "application/json");
        // start a request with retry timeout = 10 secs
        response = httpClient.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode == 200)
        {
          logger.debug("telemetry server request success: " + response);
        }
        else
        {
          logger.debug("telemetry server request error: " + response);
          success = false;
        }

        logger.debug(EntityUtils.toString(response.getEntity(), "UTF-8"));
      }
      catch (IOException e)
      {
        logger.debug(
            "Telemetry request failed, IOException" +
            "response: {}, exception: {}", response, e.getMessage());
        success = false;
      }
      finally
      {
        logger.debug("Telemetry request success={} " +
                     "and clean the current queue", success);
        // clean the current queue
        instance.queue.clear();
      }
    }
  }

  public void add(TelemetryEvent event)
  {
    if (!enabled)
    {
      return;
    }
    queue.add(event);
    if (queue.size() >= BATCH_SIZE ||
        (event.containsKey("Urgent") && (boolean) event.get("Urgent")))
    {
      // When the queue is full or the event is urgent,
      // then start a new thread to upload without blocking the current thread
      new TelemetryUploader(this).start();
    }
  }

  /**
   * force to flush events in the queue
   */
  public void flush()
  {
    if (!enabled)
    {
      return;
    }
    if (!queue.isEmpty())
    {
      // start a new thread to upload without blocking the current thread
      new TelemetryUploader(this).start();
    }
  }

  /**
   * convert a list of json objects to a string
   *
   * @param queue a list of json objects
   * @return the result json string
   */
  public String logsToString(LinkedList<TelemetryEvent> queue)
  {
    JSONArray logs = new JSONArray();
    for (TelemetryEvent event : queue)
    {
      logs.add(event);
    }
    return logs.toString();
  }

  public int size()
  {
    return queue.size();
  }

  /**
   * log error http response to telemetry
   */
  public void logHttpRequestTelemetryEvent(
      String eventName,
      HttpRequestBase request,
      int injectSocketTimeout,
      AtomicBoolean canceling,
      boolean withoutCookies,
      boolean includeRetryParameters,
      boolean includeRequestGuid,
      CloseableHttpResponse response,
      final Exception savedEx,
      String breakRetryReason,
      long retryTimeout,
      int retryCount,
      String sqlState,
      int errorCode)
  {
    if (enabled)
    {
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
      JSONObject value = new JSONObject();
      value.put("request", request.toString());
      value.put("retryTimeout", retryTimeout);
      value.put("injectSocketTimeout", injectSocketTimeout);
      value.put("canceling", canceling == null ? "null" : canceling.get());
      value.put("withoutCookies", withoutCookies);
      value.put("includeRetryParameters", includeRetryParameters);
      value.put("includeRequestGuid", includeRequestGuid);
      value.put("breakRetryReason", breakRetryReason);
      value.put("retryTimeout", retryTimeout);
      value.put("retryCount", retryCount);
      value.put("sqlState", sqlState);
      value.put("errorCode", errorCode);
      int responseStatusCode = -1;
      if (response != null)
      {
        value.put("response", response.toString());
        value.put("responseStatusLine", response.getStatusLine().toString());
        if (response.getStatusLine() != null)
        {
          responseStatusCode = response.getStatusLine().getStatusCode();
          value.put("responseStatusCode", responseStatusCode);
        }
      }
      else
      {
        value.put("response", null);
      }
      if (savedEx != null)
      {
        value.put("exceptionMessage", savedEx.getLocalizedMessage());
        StringWriter sw = new StringWriter();
        savedEx.printStackTrace(new PrintWriter(sw));
        value.put("exceptionStackTrace", sw.toString());
      }
      TelemetryEvent log = logBuilder
          .withName(eventName)
          .withValue(value)
          .withTag("sqlState", sqlState)
          .withTag("errorCode", errorCode)
          .withTag("responseStatusCode", responseStatusCode)
          .build();
      this.add(log);
    }
  }
}
