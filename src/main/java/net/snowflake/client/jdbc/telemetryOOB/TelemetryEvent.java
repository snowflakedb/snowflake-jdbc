package net.snowflake.client.jdbc.telemetryOOB;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.UUID;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.util.SFTimestamp;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.ResourceBundleManager;

/**
 * Copyright (c) 2018-2019 Snowflake Computing Inc. All rights reserved.
 *
 * <p>Telemetry Event Class
 */
public class TelemetryEvent extends JSONObject {
  private static final long serialVersionUID = 1L;
  private static final int schemaVersion = 1;

  public enum Type {
    Metric,
    Log
  }

  /** Build metric json object */
  public static class MetricBuilder extends Builder<MetricBuilder> {

    public MetricBuilder withException(Exception ex) {
      this.withName("Exception:" + ex.getMessage());
      this.withValue(1);
      return this;
    }

    public MetricBuilder() {
      super(MetricBuilder.class);
    }

    public MetricBuilder withValue(int value) {
      body.put("Value", value);
      return this;
    }

    public MetricBuilder withValue(float value) {
      body.put("Value", value);
      return this;
    }

    public TelemetryEvent build() {
      TelemetryEvent event = super.build();
      event.put("Type", Type.Metric);
      return event;
    }
  }

  /** Build log json object */
  public static class LogBuilder extends Builder<LogBuilder> {
    public LogBuilder() {
      super(LogBuilder.class);
    }

    /**
     * build a log event for an exception including the full stack trace
     *
     * @param ex The exception to build a log event
     * @return The log event builder
     */
    public LogBuilder withException(Exception ex) {
      this.withName("Exception:" + ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      String stackTrace = sw.toString(); // stack trace as a string
      this.withValue(stackTrace);
      return this;
    }

    public LogBuilder withException(final SFException ex) {
      this.withName("Exception:" + ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      String stackTrace = sw.toString(); // stack trace as a string
      this.withValue(stackTrace);
      return this;
    }

    public LogBuilder withValue(String value) {
      body.put("Value", SecretDetector.maskSecrets(value));
      return this;
    }

    public LogBuilder withValue(JSONObject value) {
      body.put("Value", SecretDetector.maskJsonObject(value));
      return this;
    }

    public LogBuilder withValue(JSONArray value) {
      body.put("Value", SecretDetector.maskJsonArray(value));
      return this;
    }

    public TelemetryEvent build() {
      TelemetryEvent event = super.build();
      event.put("Type", Type.Log);
      return event;
    }
  }

  private static class Builder<T> {
    protected final Class<T> builderClass;
    protected TelemetryEvent body = new TelemetryEvent();
    protected HashMap<String, String> tags = new HashMap<>();
    private static final String version =
        ResourceBundleManager.getSingleton("net.snowflake.client.jdbc.version")
            .getLocalizedMessage("version");
    private static final String driver = "JDBC";

    public Builder(Class<T> builderClass) {
      this.builderClass = builderClass;
      withTag("driver", driver);
      withTag("version", version);
      TelemetryService instance = TelemetryService.getInstance();
      withTag("telemetryServerDeployment", instance.getServerDeploymentName());
      withTag("connectionString", instance.getDriverConnectionString());
      JSONObject context = instance.getContext();
      if (context != null) {
        for (String key : context.keySet()) {
          Object val = context.get(key);
          if (val != null) {
            withTag(
                "ctx_" + key.toLowerCase(), SecretDetector.maskParameterValue(key, val.toString()));
          }
        }
      }
    }

    public T withName(String name) {
      body.put("Name", SecretDetector.maskSecrets(name));
      return builderClass.cast(this);
    }

    public T withTag(String name, int value) {
      return withTag(name, Integer.toString(value));
    }

    public T withTag(String name, String value) {
      if (value != null && value.length() > 0) {
        tags.put(name, SecretDetector.maskSecrets(value));
      }
      return builderClass.cast(this);
    }

    protected TelemetryEvent build() {
      body.put("UUID", UUID.randomUUID().toString());
      body.put("Created_On", SFTimestamp.getUTCNow());
      body.put("SchemaVersion", schemaVersion);
      this.putMap("Tags", tags);
      return body;
    }

    private void putMap(String name, HashMap<String, String> map) {
      JSONObject tags = new JSONObject();
      for (String key : map.keySet()) {
        tags.put(key, map.get(key));
      }
      body.put(name, tags);
    }
  }

  /** @return the deployment of this event */
  public String getDeployment() {
    JSONArray tags = (JSONArray) this.get("Tags");
    for (Object tag : tags) {
      JSONObject json = (JSONObject) tag;
      if (json.get("Name").toString().compareTo("deployment") == 0) {
        return json.get("Value").toString();
      }
    }
    return "Unknown";
  }
}
