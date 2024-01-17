package net.snowflake.client.jdbc.telemetryOOB;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.jdbc.SnowflakeConnectString;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 *
 * <p>Out of Band Telemetry Service This is a thread safe singleton queue containing telemetry
 * messages
 */
public class TelemetryService {
  private static final SFLogger logger = SFLoggerFactory.getLogger(TelemetryService.class);

  private static ThreadLocal<TelemetryService> _threadLocal =
      new ThreadLocal<TelemetryService>() {
        @Override
        protected TelemetryService initialValue() {
          return new TelemetryService();
        }
      };

  // Global parameters:
  private static final String TELEMETRY_SERVER_URL_PATTERN =
      "https://(sfcdev\\.|sfctest\\.|)client-telemetry\\.snowflakecomputing\\" + ".com/enqueue";

  /**
   * control which deployments are enabled: the service skips all events for the disabled
   * deployments
   */
  private static HashSet<String> ENABLED_DEPLOYMENT =
      new HashSet<>(
          Arrays.asList(
              TELEMETRY_SERVER_DEPLOYMENT.DEV.name,
              TELEMETRY_SERVER_DEPLOYMENT.REG.name,
              TELEMETRY_SERVER_DEPLOYMENT.QA1.name,
              TELEMETRY_SERVER_DEPLOYMENT.PREPROD3.name,
              TELEMETRY_SERVER_DEPLOYMENT.PROD.name));

  // connection string for current connection
  private String connStr = "";
  // current snowflake connection string
  private SnowflakeConnectString sfConnStr;

  /**
   * @return return thread local instance
   */
  public static TelemetryService getInstance() {
    return _threadLocal.get();
  }

  private static final int DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY = 10;

  /** the number of retry to trigger the HTTP timeout telemetry event */
  private int numOfRetryToTriggerTelemetry = DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY;
  // local parameters
  /** the context (e.g., connection properties) to be included in the telemetry events */
  private JSONObject context;

  public void resetNumOfRetryToTriggerTelemetry() {
    numOfRetryToTriggerTelemetry = DEFAULT_NUM_OF_RETRY_TO_TRIGGER_TELEMETRY;
  }

  public int getNumOfRetryToTriggerTelemetry() {
    return numOfRetryToTriggerTelemetry;
  }

  public void setNumOfRetryToTriggerTelemetry(int num) {
    numOfRetryToTriggerTelemetry = num;
  }

  private TELEMETRY_SERVER_DEPLOYMENT serverDeployment = TELEMETRY_SERVER_DEPLOYMENT.PROD;

  /**
   * control enable/disable the whole service: disabled service will skip added events and uploading
   * to the server
   */
  private static boolean enabled = true;

  private static boolean htapEnabled = false;

  private static final Object enableLock = new Object();

  private static final Object enableHTAPLock = new Object();

  public static void enable() {
    synchronized (enableLock) {
      enabled = true;
    }
  }

  public static void disable() {
    synchronized (enableLock) {
      enabled = false;
    }
  }

  public static void enableHTAP() {
    synchronized (enableHTAPLock) {
      htapEnabled = true;
    }
  }

  public static void disableHTAP() {
    synchronized (enableHTAPLock) {
      htapEnabled = false;
    }
  }

  public boolean isEnabled() {
    synchronized (enableLock) {
      return enabled;
    }
  }

  public boolean isHTAPEnabled() {
    synchronized (enableHTAPLock) {
      return htapEnabled;
    }
  }

  public JSONObject getContext() {
    return context;
  }

  /** Note: Only used for IT */
  public void updateContextForIT(Map<String, String> params) {
    Properties info = new Properties();
    for (String key : params.keySet()) {
      Object val = params.get(key);
      if (val != null) {
        info.put(key, val);
      }
    }
    SnowflakeConnectString conStr = SnowflakeConnectString.parse(params.get("uri"), info);
    this.updateContext(conStr);
  }

  public void updateContext(SnowflakeConnectString conStr) {
    if (conStr != null) {
      sfConnStr = conStr;
      configureDeployment(conStr);
      context = new JSONObject();

      for (Map.Entry<String, Object> entry : conStr.getParameters().entrySet()) {
        String k = entry.getKey();
        Object v = entry.getValue();
        if (!SecretDetector.isSensitive(k)) {
          context.put(k, v);
        }
      }
    }
  }

  private TELEMETRY_SERVER_DEPLOYMENT manuallyConfigureDeployment(String dep) {
    switch (dep) {
      case "REG":
        return TELEMETRY_SERVER_DEPLOYMENT.REG;
      case "DEV":
        return TELEMETRY_SERVER_DEPLOYMENT.DEV;
      case "QA1":
        return TELEMETRY_SERVER_DEPLOYMENT.QA1;
      case "PREPROD":
        return TELEMETRY_SERVER_DEPLOYMENT.PREPROD3;
      case "PROD":
        return TELEMETRY_SERVER_DEPLOYMENT.PROD;
      default:
        return null;
    }
  }

  /**
   * configure telemetry deployment based on connection url and info Note: it is not thread-safe
   * while connecting to different deployments simultaneously.
   *
   * @param conStr Connect String
   */
  private void configureDeployment(SnowflakeConnectString conStr) {
    if (!conStr.isValid()) {
      return;
    }
    connStr = conStr.toString();
    String account = conStr.getAccount();
    int port = conStr.getPort();
    // default value
    TELEMETRY_SERVER_DEPLOYMENT deployment = TELEMETRY_SERVER_DEPLOYMENT.PROD;

    Map<String, Object> conParams = conStr.getParameters();
    if (conParams.containsKey("TELEMETRYDEPLOYMENT")) {
      String conDeployment =
          String.valueOf(conParams.get("TELEMETRYDEPLOYMENT")).trim().toUpperCase();
      deployment = manuallyConfigureDeployment(conDeployment);
      if (deployment != null) {
        this.setDeployment(deployment);
        return;
      }
    }
    if (conStr.getHost().contains("reg") || conStr.getHost().contains("local")) {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.REG;
      if (port == 8080) {
        deployment = TELEMETRY_SERVER_DEPLOYMENT.DEV;
      }
    } else if (conStr.getHost().contains("qa1") || account.contains("qa1")) {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.QA1;
    } else if (conStr.getHost().contains("preprod3")) {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.PREPROD3;
    } else if (conStr.getHost().contains("snowflake.temptest")) {
      deployment = TELEMETRY_SERVER_DEPLOYMENT.QA1;
    }
    this.setDeployment(deployment);
  }

  /** whether the telemetry service is enabled for current deployment */
  public boolean isDeploymentEnabled() {
    return ENABLED_DEPLOYMENT.contains(this.serverDeployment.name);
  }

  public String getDriverConnectionString() {
    return this.connStr;
  }

  public SnowflakeConnectString getSnowflakeConnectionString() {
    return sfConnStr;
  }

  private enum TELEMETRY_API {
    SFCTEST(
        "https://sfctest.client-telemetry.snowflakecomputing.com/enqueue",
        "rRNY3EPNsB4U89XYuqsZKa7TSxb9QVX93yNM4tS6"), // pragma: allowlist secret
    SFCDEV(
        "https://sfcdev.client-telemetry.snowflakecomputing.com/enqueue",
        "kyTKLWpEZSaJnrzTZ63I96QXZHKsgfqbaGmAaIWf"), // pragma: allowlist secret
    PROD(
        "https://client-telemetry.snowflakecomputing.com/enqueue",
        "wLpEKqnLOW9tGNwTjab5N611YQApOb3t9xOnE1rX"); // pragma: allowlist secret

    private final String url;

    // Note that this key is public available and only used as usage plan for
    // throttling
    private final String apiKey;

    TELEMETRY_API(String host, String key) {
      this.url = host;
      this.apiKey = key;
    }
  }

  public enum TELEMETRY_SERVER_DEPLOYMENT {
    DEV("dev", TELEMETRY_API.SFCDEV),
    REG("reg", TELEMETRY_API.SFCDEV),
    QA1("qa1", TELEMETRY_API.SFCDEV),
    PREPROD3("preprod3", TELEMETRY_API.SFCDEV),
    PROD("prod", TELEMETRY_API.PROD);

    private String name;
    private String url;
    private final String apiKey;

    TELEMETRY_SERVER_DEPLOYMENT(String name, TELEMETRY_API api) {
      this.name = name;
      this.url = api.url;
      this.apiKey = api.apiKey;
    }

    public String getURL() {
      return url;
    }

    public String getName() {
      return name;
    }

    public String getApiKey() {
      return apiKey;
    }

    public void setURL(String url) {
      this.url = url;
    }
  }

  public void setDeployment(TELEMETRY_SERVER_DEPLOYMENT deployment) {
    serverDeployment = deployment;
  }

  public String getServerDeploymentName() {
    return serverDeployment.name;
  }

  private AtomicInteger eventCnt = new AtomicInteger();

  private AtomicInteger clientFailureCnt = new AtomicInteger();

  private AtomicInteger serverFailureCnt = new AtomicInteger();

  private String lastClientError = "";

  /**
   * @return the number of events successfully reported by this service
   */
  public int getEventCount() {
    return eventCnt.get();
  }

  /**
   * @return the number of times an event was attempted to be reported but failed due to a
   *     client-side error
   */
  public int getClientFailureCount() {
    return clientFailureCnt.get();
  }

  /**
   * @return the number of times an event was attempted to be reported but failed due to a
   *     server-side error
   */
  public int getServerFailureCount() {
    return serverFailureCnt.get();
  }

  /**
   * @return the string containing the most recent failed response
   */
  public String getLastClientError() {
    return this.lastClientError;
  }

  /** Count one more successfully reported events */
  public void count() {
    eventCnt.incrementAndGet();
  }

  /** Report the event to the telemetry server in a new thread */
  public void report(TelemetryEvent event) {
    reportChooseEvent(event, /* isHTAP */ false);
  }

  public void reportChooseEvent(TelemetryEvent event, boolean isHTAP) {
    if ((!enabled && !isHTAP) || (!htapEnabled && isHTAP) || event == null || event.isEmpty()) {
      return;
    }

    // Start a new thread to upload without blocking the current thread
    Runnable runUpload =
        new TelemetryUploader(
            this, exportQueueToString(event), exportQueueToLogString(event), isHTAP);
    TelemetryThreadPool.getInstance().execute(runUpload);
  }

  /** Convert an event to a payload in string */
  public String exportQueueToString(TelemetryEvent event) {
    JSONArray logs = new JSONArray();
    logs.add(event);
    return logs.toString();
  }

  public String exportQueueToLogString(TelemetryEvent event) {
    JSONArray logs = new JSONArray();
    logs.add(event);
    return JSONArray.toJSONString(logs, new SecretDetector.SecretDetectorJSONStyle());
  }

  static class TelemetryUploader implements Runnable {
    private TelemetryService instance;
    private String payload;
    private String payloadLogStr;
    private boolean isHTAP;
    private static final int TIMEOUT = 5000; // 5 second timeout limit
    private static final RequestConfig config =
        RequestConfig.custom()
            .setConnectionRequestTimeout(TIMEOUT)
            .setConnectionRequestTimeout(TIMEOUT)
            .setSocketTimeout(TIMEOUT)
            .build();

    public TelemetryUploader(
        TelemetryService _instance, String _payload, String _payloadLogStr, boolean _isHTAP) {
      instance = _instance;
      payload = _payload;
      payloadLogStr = _payloadLogStr;
      isHTAP = _isHTAP;
    }

    public void run() {
      if (!isHTAP && !instance.enabled) {
        return;
      }

      if (isHTAP && !instance.htapEnabled) {
        return;
      }

      if (!instance.isDeploymentEnabled()) {
        // skip the disabled deployment
        logger.debug("skip the disabled deployment: ", instance.serverDeployment.name);
        return;
      }

      if (!instance.serverDeployment.url.matches(TELEMETRY_SERVER_URL_PATTERN)) {
        // skip the disabled deployment
        logger.debug("ignore invalid url: ", instance.serverDeployment.url);
        return;
      }

      uploadPayload();
    }

    private void uploadPayload() {
      logger.debugNoMask("Running telemetry uploader. The payload is: " + payloadLogStr);
      CloseableHttpResponse response = null;
      boolean success = true;

      try {
        HttpPost post = new HttpPost(instance.serverDeployment.url);
        post.setEntity(new StringEntity(payload));
        post.setHeader("Content-type", "application/json");
        post.setHeader("x-api-key", instance.serverDeployment.getApiKey());
        try (CloseableHttpClient httpClient =
            HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
          response = httpClient.execute(post);
          int statusCode = response.getStatusLine().getStatusCode();

          if (statusCode == 200) {
            logger.debug("telemetry server request success: {}", response, true);
            instance.count();
          } else if (statusCode == 429) {
            logger.debug("telemetry server request hit server cap on response: {}", response);
            instance.serverFailureCnt.incrementAndGet();
          } else {
            logger.debug("telemetry server request error: {}", response, true);
            instance.lastClientError = response.toString();
            instance.clientFailureCnt.incrementAndGet();
            success = false;
          }
          logger.debug(EntityUtils.toString(response.getEntity(), "UTF-8"), true);
          response.close();
        }
      } catch (Exception e) {
        // exception from here is always captured
        logger.debug(
            "Telemetry request failed, Exception" + "response: {}, exception: {}",
            response,
            e.getMessage());
        String res = "null";
        if (response != null) {
          res = response.toString();
        }
        instance.lastClientError = "Response: " + res + "; Error: " + e.getMessage();
        instance.clientFailureCnt.incrementAndGet();
        success = false;
      } finally {
        logger.debug("Telemetry request success={} " + "and clean the current queue", success);
      }
    }
  }

  /** log OCSP exception to telemetry */
  public void logOCSPExceptionTelemetryEvent(
      String eventType, JSONObject telemetryData, CertificateException ex) {
    if (enabled) {
      String eventName = "OCSPException";
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();

      if (ex != null) {
        telemetryData.put("exceptionMessage", ex.getLocalizedMessage());
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        telemetryData.put("exceptionStackTrace", sw.toString());
      }

      TelemetryEvent log =
          logBuilder
              .withName(eventName)
              .withValue(telemetryData)
              .withTag("eventType", eventType)
              .build();
      this.report(log);
    }
  }

  /** log error http response to telemetry */
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
      int errorCode) {

    if (enabled) {
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
      if (response != null) {
        value.put("response", response.toString());
        value.put("responseStatusLine", response.getStatusLine().toString());
        if (response.getStatusLine() != null) {
          responseStatusCode = response.getStatusLine().getStatusCode();
          value.put("responseStatusCode", responseStatusCode);
        }
      } else {
        value.put("response", null);
      }
      if (savedEx != null) {
        value.put("exceptionMessage", savedEx.getLocalizedMessage());
        StringWriter sw = new StringWriter();
        savedEx.printStackTrace(new PrintWriter(sw));
        value.put("exceptionStackTrace", sw.toString());
      }
      TelemetryEvent log =
          logBuilder
              .withName(eventName)
              .withValue(value)
              .withTag("sqlState", sqlState)
              .withTag("errorCode", errorCode)
              .withTag("responseStatusCode", responseStatusCode)
              .build();
      this.report(log);
    }
  }

  /** log execution times from various processing slices */
  public void logExecutionTimeTelemetryEvent(JSONObject telemetryData, String eventName) {
    if (htapEnabled) {
      TelemetryEvent.LogBuilder logBuilder = new TelemetryEvent.LogBuilder();
      TelemetryEvent log =
          logBuilder
              .withName(eventName)
              .withValue(telemetryData)
              .withTag("eventType", eventName)
              .build();
      this.reportChooseEvent(log, /* isHTAP */ true);
    }
  }
}
