package net.snowflake.client.internal.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import net.snowflake.client.internal.core.ConnectionIdentifierShape;

public class ConnectionParameters {
  private final String url;
  private final Properties params;
  private final List<String> deferredLogMessages = new ArrayList<>();

  // TODO(SNOW-3548350): captured at the moment the user input is available (TOML config or raw
  //  URL+Properties), before any host synthesis or normalization, and consumed by
  //  ConnectionIdentifierShapeTelemetry.emit() after a successful login. Remove together with the
  //  rest of the connection-identifier-shape telemetry plumbing.
  private ConnectionIdentifierShape connectionIdentifierShape;

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

  public ConnectionIdentifierShape getConnectionIdentifierShape() {
    return connectionIdentifierShape;
  }

  public void setConnectionIdentifierShape(ConnectionIdentifierShape shape) {
    this.connectionIdentifierShape = shape;
  }
}
