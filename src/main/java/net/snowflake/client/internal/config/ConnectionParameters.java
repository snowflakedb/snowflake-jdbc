package net.snowflake.client.internal.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConnectionParameters {
  private final String url;
  private final Properties params;
  private final List<String> deferredLogMessages = new ArrayList<>();

  public ConnectionParameters(String uri, Properties params) {
    this.url = uri;
    this.params = params;
  }

  public String getUrl() {
    return url;
  }

  public Properties getParams() {
    return params;
  }

  public void addDeferredLogMessage(String message) {
    deferredLogMessages.add(message);
  }

  public List<String> getDeferredLogMessages() {
    return deferredLogMessages;
  }
}
