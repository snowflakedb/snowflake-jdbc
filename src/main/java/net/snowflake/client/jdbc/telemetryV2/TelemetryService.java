package net.snowflake.client.jdbc.telemetryV2;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.jdbc.SnowflakeSQLException;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 * <p>
 * Out of Band Telemetry Service
 * This is a singleton queue containing telemetry messages
 */
public class TelemetryService
{
  private static final SFLogger logger =
      SFLoggerFactory.getLogger(TelemetryService.class);

  private static LinkedList<TelemetryEvent> queue = new LinkedList<>();
  private static TelemetryService telemetryService = null;
  private static int BATCH_SIZE = 100;
  private static boolean enabled = false;
  private static TELEMETRY_SERVER_DEPLOYMENT serverDeployment;

  /**
   * configure telemetry deployment based on connection url and info
   * Note: it is not thread-safe while connecting to different deployments
   * simultaneously.
   * @param url
   * @param account
   */
  public static void configureDeployment(final String url,
                                         final String account,
                                         final String port)
  {
    TelemetryService instance = getInstance();
    // default value
    TELEMETRY_SERVER_DEPLOYMENT deployment = TELEMETRY_SERVER_DEPLOYMENT.PROD;
    if (url != null)
    {
      if (url.contains("reg"))
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
    instance.setDeployment(deployment);
  }

  private enum TELEMETRY_SERVER_URL
  {
    SFCTEST ("https://lximwp8945.execute-api.us-west-2.amazonaws.com/sfctest/enqueue"),
    SFCDEV ("https://lol6l3j52m.execute-api.us-west-2.amazonaws.com/sfcdev/enqueue"),
    US2 ("https://lol6l3j52m.execute-api.us-west-2.amazonaws.com/sfctest/enqueue"); // todo: change the url

    private final String url;

    TELEMETRY_SERVER_URL(String url)
    {
      this.url = url;
    }
  }

  public enum TELEMETRY_SERVER_DEPLOYMENT
  {
    DEV ("dev", TELEMETRY_SERVER_URL.SFCTEST.url),
    REG ("reg", TELEMETRY_SERVER_URL.SFCTEST.url),
    QA1 ("qa1", TELEMETRY_SERVER_URL.SFCDEV.url),
    PREPROD2 ("preprod2", TELEMETRY_SERVER_URL.SFCDEV.url),
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

  public static synchronized TelemetryService getInstance()
  {
    if (telemetryService == null)
    {
      telemetryService = new TelemetryService();
      Runtime.getRuntime().addShutdownHook(new TelemetryUploader());
    }
    return telemetryService;
  }

  public synchronized void setDeployment(TELEMETRY_SERVER_DEPLOYMENT deployment)
  {
    serverDeployment = deployment;
  }

  public String getServerDeploymentName()
  {
    return serverDeployment.name;
  }

  public synchronized void enable()
  {
    enabled = true;
  }

  public synchronized void disable()
  {
    enabled = false;
  }

  public synchronized boolean isEnabled()
  {
    return enabled;
  }

  static class TelemetryUploader extends Thread
  {
    public void run() {
      if (!enabled)
      {
        return;
      }
      synchronized (queue)
      {
        if (queue.isEmpty())
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
    }

    private void flushQueue(){
      String response = null;
      boolean success = true;
      try
      {
        HttpPost post = new HttpPost(serverDeployment.url);
        post.setEntity(new StringEntity(logsToString(queue)));
        post.setHeader("Content-type", "application/json");
        // start a request with retry timeout = 10 secs
        response = HttpUtil.executeRequest(post, 10, 0, null);
        JSONParser p = new JSONParser();
        JSONObject obj = (JSONObject) p.parse(response);
        if (obj != null && obj.getAsNumber("statusCode").intValue() == 200)
        {
          logger.debug("telemetry server request success: " + response);
        }
        else
        {
          logger.warn("telemetry server request error: " + response);
          success = false;
        }

        logger.debug(response);
      }
      catch (SnowflakeSQLException e)
      {
        logger.warn(
            "Telemetry request failed, SnowflakeSQLException " +
                "response: {}, exception: {}", response, e.getMessage());
        success = false;
      }
      catch (IOException e)
      {
        logger.warn(
            "Telemetry request failed, IOException" +
                "response: {}, exception: {}", response, e.getMessage());
        success = false;
      }
      catch (ParseException pe)
      {
        logger.warn(
            "Telemetry request failed, ParseException" +
                "response: {}, exception: {}", response, pe.getMessage());
        success = false;
      }
      finally
      {
        logger.info("Telemetry request success={} " +
            "and clean the current queue", success);
        // clean the current queue
        queue.clear();
      }
    }
  }

  public void add(TelemetryEvent event)
  {
    if (!enabled)
    {
      return;
    }
    synchronized (queue)
    {
      queue.add(event);
      if (queue.size() >= BATCH_SIZE)
      {
        // start a new thread to upload without blocking the current thread
        new TelemetryUploader().start();
      }
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
    synchronized (queue)
    {
      if (!queue.isEmpty())
      {
        // start a new thread to upload without blocking the current thread
        new TelemetryUploader().start();
      }
    }
  }

  /**
   * convert a list of json objects to a string
   *
   * @param queue a list of json objects
   * @return the result json string
   */
  static String logsToString(LinkedList<TelemetryEvent> queue)
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
}
