package net.snowflake.client.jdbc.telemetryV2;

import net.minidev.json.JSONArray;
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
  private static String SERVER_URL = "https://syswm3rhj6.execute-api.us-west-2.amazonaws.com/sfctest/enqueue";
  private static int BATCH_SIZE = 100;
  private static boolean enabled = false;

  public static synchronized TelemetryService getInstance()
  {
    if (telemetryService == null)
    {
      telemetryService = new TelemetryService();
      Runtime.getRuntime().addShutdownHook(new TelemetryUploader());
    }
    return telemetryService;
  }

  public synchronized void setServerURL(String url)
  {
    SERVER_URL = url;
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
        HttpPost post = new HttpPost(SERVER_URL);
        post.setEntity(new StringEntity(logsToString(queue)));
        post.setHeader("Content-type", "application/json");
        // start a request with retry timeout = 10 secs
        response = HttpUtil.executeRequest(post, 10, 0, null);
        logger.debug(response);
      }
      catch (SnowflakeSQLException e)
      {
        logger.error(
            "Telemetry request failed, SnowflakeSQLException " +
                "response: {}, exception: {}", response, e.getMessage());
        success = false;
      }
      catch (IOException e)
      {
        logger.error(
            "Telemetry request failed, IOException" +
                "response: {}, exception: {}", response, e.getMessage());
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
