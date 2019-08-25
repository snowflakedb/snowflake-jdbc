package net.snowflake.client.jdbc.telemetryOOB;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFTrustManager;
import net.snowflake.client.jdbc.SnowflakeConnectString;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  private static final String TELEMETRY_SERVER_URL_PATTERN
      = "https://(sfcdev\\.|sfctest\\.|)client-telemetry\\.snowflakecomputing\\.com/enqueue";

  // always flush queue when receiving revoked OCSP exception
  public static boolean FLUSH_OCSP_REVOKED_EVENT = true;
  /**
   * control which deployments are enabled:
   * the service skips all events for the disabled deployments
   */
  private static HashSet<String> ENABLED_DEPLOYMENT
      = new HashSet<>(Arrays.asList(
      TELEMETRY_SERVER_DEPLOYMENT.DEV.name,
      TELEMETRY_SERVER_DEPLOYMENT.REG.name,
      TELEMETRY_SERVER_DEPLOYMENT.QA1.name,
      TELEMETRY_SERVER_DEPLOYMENT.PREPROD2.name,
      TELEMETRY_SERVER_DEPLOYMENT.PROD.name
  ));

  // connection string for current connection
  private String connStr = "";
  // current snowflake connection string
  private SnowflakeConnectString sfConnStr;

  /**
   * @return return thread local instance
   */
  public static TelemetryService getInstance()
  {
    return _threadLocal.get();
  }

  private static final int DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY = 10;
  private static final int DEFAULT_BATCH_SIZE = 100;
  /**
   * the number of retry to trigger the HTTP timeout telemetry event
   */
  private int numOfRetryToTriggerTelemetry = DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY;
  // local parameters
  private ConcurrentLinkedQueue<TelemetryEvent> queue
      = new ConcurrentLinkedQueue<>();
  private int batchSize = DEFAULT_BATCH_SIZE;
  /**
   * the context (e.g., connection properties) to be included
   * in the telemetry events
   */
  private JSONObject context;

  private TelemetryService()
  {
    try
    {
      SecurityManager sm = System.getSecurityManager();
      if (sm != null)
      {
        sm.checkPermission(new RuntimePermission("shutdownHooks"));
      }

      Runtime.getRuntime().addShutdownHook(new Thread(
          new TelemetryUploader(this, exportQueueToString())));
    }
    catch (SecurityException e)
    {
      logger.debug("Failed to add shutdown hook for telemetry service");
    }
  }

  private ExecutorService uploader = Executors.newFixedThreadPool(3);

  public void resetNumOfRetryToTriggerTelemetry()
  {
    numOfRetryToTriggerTelemetry = DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY;
  }

  public int getNumOfRetryToTriggerTelemetry()
  {
    return numOfRetryToTriggerTelemetry;
  }

  public void setNumOfRetryToTriggerTelemetry(int num)
  {
    numOfRetryToTriggerTelemetry = num;
  }

  private TELEMETRY_SERVER_DEPLOYMENT serverDeployment;

  /**
   * control enable/disable the whole service:
   * disabled service will skip added events and uploading to the server
   */
  private boolean enabled = true;

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
   * Try to flush events in the queue before throwing exceptions
   * It's true by default and only set to false for testing
   */
  private boolean runFlushBeforeException = true;

  public boolean runFlushBeforeException()
  {
    return runFlushBeforeException;
  }

  public void enableRunFlushBeforeException()
  {
    runFlushBeforeException = true;
  }

  public void disableRunFlushBeforeException()
  {
    runFlushBeforeException = false;
  }

  public JSONObject getContext()
  {
    return context;
  }

  /**
   * Note: Only used for IT
   */
  public void updateContextForIT(Map<String, String> params)
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
    SnowflakeConnectString conStr = SnowflakeConnectString.parse(params.get("uri"), info);
    this.updateContext(conStr);
  }

  public void updateContext(SnowflakeConnectString conStr)
  {
    if (conStr != null)
    {
      sfConnStr = conStr;
      configureDeployment(conStr);
      context = new JSONObject();

      for (Map.Entry<String, Object> entry : conStr.getParameters().entrySet())
      {
        String k = entry.getKey();
        Object v = entry.getValue();
        if (!SecretDetector.isSensitive(k))
        {
          context.put(k, v);
        }
      }
    }
  }

  /**
   * configure telemetry deployment based on connection url and info
   * Note: it is not thread-safe while connecting to different deployments
   * simultaneously.
   *
   * @param conStr Connect String
   */
  private void configureDeployment(SnowflakeConnectString conStr)
  {
    if (!conStr.isValid())
    {
      return;
    }
    connStr = conStr.toString();
    String account = conStr.getAccount();
    int port = conStr.getPort();
    // default value
    TELEMETRY_SERVER_DEPLOYMENT deployment = TELEMETRY_SERVER_DEPLOYMENT.PROD;
    if (conStr.getHost().contains("reg") || conStr.getHost().contains("local"))
    {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.REG;
      if (port == 8080)
      {
        deployment = TELEMETRY_SERVER_DEPLOYMENT.DEV;
      }
    }
    else if (conStr.getHost().contains("qa1") || account.contains("qa1"))
    {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.QA1;
    }
    else if (conStr.getHost().contains("preprod2"))
    {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.PREPROD2;
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

  public TelemetryEvent peek()
  {
    return queue.peek();
  }

  public void setBatchSize(int size)
  {
    this.batchSize = size;
  }

  public void resetBatchSize()
  {
    this.batchSize = DEFAULT_BATCH_SIZE;
  }

  public String getDriverConnectionString()
  {
    return this.connStr;
  }

  public SnowflakeConnectString getSnowflakeConnectionString()
  {
    return sfConnStr;
  }

  private enum TELEMETRY_API
  {
    SFCTEST("https://sfctest.client-telemetry.snowflakecomputing.com/enqueue",
            "rRNY3EPNsB4U89XYuqsZKa7TSxb9QVX93yNM4tS6"),
    SFCDEV("https://sfcdev.client-telemetry.snowflakecomputing.com/enqueue",
           "kyTKLWpEZSaJnrzTZ63I96QXZHKsgfqbaGmAaIWf"),
    PROD("https://client-telemetry.snowflakecomputing.com/enqueue",
         "wLpEKqnLOW9tGNwTjab5N611YQApOb3t9xOnE1rX");

    private final String url;

    // Note that this key is public available and only used as usage plan for throttling
    private final String apiKey;

    TELEMETRY_API(String host, String key)
    {
      this.url = host;
      this.apiKey = key;
    }
  }

  public enum TELEMETRY_SERVER_DEPLOYMENT
  {
    DEV("dev", TELEMETRY_API.SFCTEST),
    REG("reg", TELEMETRY_API.SFCTEST),
    QA1("qa1", TELEMETRY_API.SFCDEV),
    PREPROD2("preprod2", TELEMETRY_API.SFCDEV),
    PROD("prod", TELEMETRY_API.PROD);

    private String name;
    private String url;
    private final String apiKey;

    TELEMETRY_SERVER_DEPLOYMENT(String name, TELEMETRY_API api)
    {
      this.name = name;
      this.url = api.url;
      this.apiKey = api.apiKey;
    }

    public String getURL()
    {
      return url;
    }

    public String getName()
    {
      return name;
    }

    public String getApiKey()
    {
      return apiKey;
    }

    public void setURL(String url)
    {
      this.url = url;
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

  public void add(TelemetryEvent event)
  {
    if (!enabled)
    {
      return;
    }
    queue.offer(event);
    if (queue.size() >= batchSize ||
        (event.containsKey("Urgent") && (boolean) event.get("Urgent")))
    {
      // When the queue is full or the event is urgent,
      // then start a new thread to upload without blocking the current thread
      Runnable runUpload = new TelemetryUploader(this, exportQueueToString());
      uploader.execute(runUpload);
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
      Runnable runUpload = new TelemetryUploader(this, exportQueueToString());
      uploader.execute(runUpload);
    }
  }

  /**
   * convert a list of json objects to a string
   *
   * @return the result json string
   */
  public String exportQueueToString()
  {
    JSONArray logs = new JSONArray();
    while (!queue.isEmpty())
    {
      logs.add(queue.poll());
    }
    return SecretDetector.maskSecrets(logs.toString());
  }

  static class TelemetryUploader implements Runnable
  {
    TelemetryService instance;
    String payload;
    RequestConfig config;
    static final int TIMEOUT = 3000; // 3 second timeout limit

    public TelemetryUploader(TelemetryService _instance, String _payload)
    {
      instance = _instance;
      payload = _payload;
      config = RequestConfig.custom()
          .setConnectionRequestTimeout(TIMEOUT)
          .setConnectionRequestTimeout(TIMEOUT)
          .setSocketTimeout(TIMEOUT)
          .build();
    }

    public void run()
    {
      if (!instance.enabled)
      {
        return;
      }
      if (payload == null || payload.equals("[]") || payload.isEmpty())
      {
        logger.debug("skip to run telemetry uploader for empty payload");
      }
      else
      {
        // flush the queue
        uploadPayload();
        logger.debug("run telemetry uploader");
      }
    }

    private void uploadPayload()
    {
      HttpResponse response = null;
      boolean success = true;
      try
      {
        if (!instance.isDeploymentEnabled())
        {
          // skip the disabled deployment
          logger.debug("skip the disabled deployment: ", instance.serverDeployment.name);
          return;
        }

        if (!instance.serverDeployment.url.matches(TELEMETRY_SERVER_URL_PATTERN))
        {
          // skip the disabled deployment
          logger.debug("ignore invalid url: ", instance.serverDeployment.url);
          return;
        }

        HttpPost post = new HttpPost(instance.serverDeployment.url);
        post.setEntity(new StringEntity(payload));
        post.setHeader("Content-type", "application/json");
        post.setHeader("x-api-key", instance.serverDeployment.getApiKey());
        // start a request with retry timeout = 3 secs
        HttpClient httpClient = HttpClientBuilder.create()
            .setDefaultRequestConfig(config)
            .build();
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
      catch (Exception e)
      {
        // exception from here is always captured
        logger.debug(
            "Telemetry request failed, Exception" +
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

  public int size()
  {
    return queue.size();
  }

  /**
   * log OCSP exception to telemetry
   */
  public void logOCSPExceptionTelemetryEvent(
      String eventType,
      JSONObject telemetryData,
      CertificateException ex)
  {
    if (enabled)
    {
      String eventName = "OCSPException";
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();

      if (ex != null)
      {
        telemetryData.put("exceptionMessage", ex.getLocalizedMessage());
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        telemetryData.put("exceptionStackTrace", sw.toString());
      }

      TelemetryEvent log = logBuilder
          .withName(eventName)
          .withValue(telemetryData)
          .withTag("eventType", eventType)
          .build();
      this.add(log);
      if (FLUSH_OCSP_REVOKED_EVENT &&
          eventType.equals(SFTrustManager.SF_OCSP_EVENT_TYPE_REVOKED_CERTIFICATE_ERROR))
      {
        this.flush();
      }
    }
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
