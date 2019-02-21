package net.snowflake.client.jdbc.telemetryOOB;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.SessionUtilExternalBrowser;
import net.snowflake.common.core.ResourceBundleManager;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Copyright (c) 2018 Snowflake Computing Inc. All rights reserved.
 * <p>
 * Telemetry Event Class
 */
public class TelemetryEvent extends JSONObject
{

  public enum Type{
      Metric, Log
  }

  /**
   * Build metric json object
   */
  public static class MetricBuilder extends Builder<MetricBuilder> {


    public MetricBuilder withException(Exception ex)
    {
      this.withName("Exception:"+ex.getMessage());
      this.withValue(1);
      return this;
    }

    public MetricBuilder()
    {
      super(MetricBuilder.class);
    }

    public MetricBuilder withValue(int value)
    {
      body.put("Value", value);
      return this;
    }

    public MetricBuilder withValue(float value)
    {
      body.put("Value", value);
      return this;
    }

    public TelemetryEvent build()
    {
      TelemetryEvent event = super.build();
      event.put("Type", Type.Metric);
      return event;
    }
  }



  /**
   * Build log json object
   */
  public static class LogBuilder extends Builder<LogBuilder> {
    public LogBuilder(){
      super(LogBuilder.class);
    }

    /**
     * build a log event for an exception including the full stack trace
     * @param ex
     * @return
     */
    public LogBuilder withException(Exception ex)
    {
      this.withName("Exception:"+ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      String stackTrace = sw.toString(); // stack trace as a string
      this.withValue(stackTrace);
      return this;
    }

    public LogBuilder withException(final SFException ex)
    {
      this.withName("Exception:"+ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      String stackTrace = sw.toString(); // stack trace as a string
      this.withValue(stackTrace);
      return this;
    }

    public LogBuilder withValue(String value)
    {
      body.put("Value", value);
      return this;
    }

    public LogBuilder withValue(JSONObject value)
    {
      body.put("Value", value);
      return this;
    }

    public LogBuilder withValue(JSONArray value)
    {
      body.put("Value", value);
      return this;
    }

    public TelemetryEvent build()
    {
      TelemetryEvent event = super.build();
      event.put("Type", Type.Log);
      return event;
    }


  }

  private static class Builder<T> {
    protected final Class<T> builderClass;
    protected TelemetryEvent body;
    protected HashMap<String, String> tags;
    private static final String version =
        ResourceBundleManager.getSingleton("net.snowflake.client.jdbc.version")
            .getLocalizedMessage("version");
    private static final String driver = "JDBC";


    public Builder(Class<T> builderClass)
    {
      body = new TelemetryEvent();
      this.builderClass = builderClass;
      tags = new HashMap();
      tags.put("driver", driver);
      Package pkg = Package.getPackage("net.snowflake.client.jdbc");
      tags.put("version", version);
      TelemetryService instance = TelemetryService.getInstance();
      tags.put("deployment", instance.getServerDeploymentName());
      JSONObject context = instance.getContext();
      if(context!=null)
      {
        for (String key : context.keySet())
        {
          Object val = context.get(key);
          if (val != null)
          {
            tags.put("ctx_"+key, val.toString());
          }
        }
      }
    }

    private String getUTCNow()
    {
      SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      dateFormatGmt.setTimeZone(TimeZone.getTimeZone("GMT"));

      //Time in GMT
      return dateFormatGmt.format(new Date());
    }

    public T withName(String name)
    {
      body.put("Name", name);
      return builderClass.cast(this);
    }

    public T withTag(String name, int value)
    {
      return withTag(name, Integer.toString(value));
    }

    public T withTag(String name, String value)
    {
      if (value != null && value.length()>0)
      {
        tags.put(name, value);
      }
      return builderClass.cast(this);
    }

    public T withUrgent(boolean urgent)
    {
      body.put("Urgent", urgent);
      return builderClass.cast(this);
    }

    protected TelemetryEvent build()
    {
      body.put("UUID", UUID.randomUUID().toString());
      body.put("Created_On", getUTCNow());
      this.putMap("Tags", tags);
      return body;
    }

    private void putMap(String name, HashMap<String, String> map)
    {
      JSONArray array = new JSONArray();
      Iterator it = map.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry pairs = (Map.Entry) it.next();
        JSONObject obj = new JSONObject();
        obj.put("Name", pairs.getKey());
        obj.put("Value", pairs.getValue());
        array.appendElement(obj);
      }
      body.put(name, array);
    }
  }

  /**
   *
   * @return the deployment of this event
   */
  public String getDeployment()
  {
     JSONArray tags = (JSONArray) this.get("Tags");
     for (Object tag : tags)
     {
       JSONObject json = (JSONObject) tag;
       if (json.get("Name").toString().compareTo("deployment")==0){
         return json.get("Value").toString();
       }
     }
     return "Unknown";
  }
}
