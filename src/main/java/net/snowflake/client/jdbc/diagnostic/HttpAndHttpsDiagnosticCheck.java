package net.snowflake.client.jdbc.diagnostic;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class HttpAndHttpsDiagnosticCheck extends DiagnosticCheck {

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(HttpAndHttpsDiagnosticCheck.class);
  final String name = "HTTP/S Connection";
  final String HTTP_SCHEMA = "http://";
  final String HTTPS_SCHEMA = "https://";

  HttpAndHttpsDiagnosticCheck(ProxyConfig proxyConfig) {
    super("HTTP/HTTPS Connection Test", proxyConfig);
  }

  @Override
  public void run(SnowflakeEndpoint snowflakeEndpoint) {
    super.run(snowflakeEndpoint);
    // We have to replace underscores with hyphens because the JDK doesn't allow underscores in the
    // hostname
    String hostname = snowflakeEndpoint.getHost().replace('_', '-');
    try {
      Proxy proxy = this.proxyConf.getProxy(snowflakeEndpoint);
      StringBuilder sb = new StringBuilder();
      String urlString =
          (snowflakeEndpoint.isSslEnabled()) ? HTTPS_SCHEMA + hostname : HTTP_SCHEMA + hostname;
      URL url = new URL(urlString);
      HttpURLConnection con =
          (snowflakeEndpoint.isSslEnabled())
              ? (HttpsURLConnection) url.openConnection(proxy)
              : (HttpURLConnection) url.openConnection(proxy);
      logger.debug("Response from server: {} {}", con.getResponseCode(), con.getResponseMessage());
      sb.append("Headers:\n");

      Map<String, List<String>> headerFields = con.getHeaderFields();
      for (Map.Entry<String, List<String>> header : headerFields.entrySet())
        sb.append(header.getKey()).append(": ").append(header.getValue()).append("\n");

      logger.debug(sb.toString());

    } catch (MalformedURLException e) {
      logger.error(
          "The URL format is incorrect, please check your allowlist JSON file for errors.");
      logger.error(e.getLocalizedMessage(), e);
    } catch (IOException e) {
      logger.error("Could not send an HTTP/HTTPS request to host {}", hostname);
      logger.error(e.getLocalizedMessage(), e);
      this.success = false;
    }
  }
}
